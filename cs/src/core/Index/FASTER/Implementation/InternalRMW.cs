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
        /// <param name="lsn">Operation serial number</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully updated (or inserted).</term>
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
        internal OperationStatus InternalRMW<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Output output, ref Context userContext, 
                                    ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession, long lsn)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var latchOperation = LatchOperation.None;
            var latchDestination = LatchDestination.NormalProcessing;

            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));

            if (fasterSession.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            FindOrCreateTag(ref stackCtx.hei, hlog.BeginAddress);
            stackCtx.SetRecordSourceToHashEntry(hlog);

            RecordInfo dummyRecordInfo = new() { Valid = true };
            ref RecordInfo srcRecordInfo = ref TryFindRecordInMemory(ref key, ref stackCtx, hlog.HeadAddress)
                ? ref stackCtx.recSrc.GetInfo()
                : ref dummyRecordInfo;
            if (srcRecordInfo.IsClosed)
                return OperationStatus.RETRY_LATER;

            // These track the latest main-log address in the tag chain; InternalContinuePendingRMW uses them to check for new inserts.
            pendingContext.InitialEntryAddress = stackCtx.hei.Address;
            pendingContext.InitialLatestLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;

            RMWInfo rmwInfo = new()
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
                    latchDestination = CheckCPRConsistencyRMW(fasterSession.Ctx.phase, ref stackCtx, ref status, ref latchOperation);
                    if (latchDestination == LatchDestination.Retry)
                        goto LatchRelease;
                }

                if (stackCtx.recSrc.LogicalAddress >= hlog.ReadOnlyAddress && latchDestination == LatchDestination.NormalProcessing)
                {
                    // Mutable Region: Update the record in-place. We perform mutable updates only if we are in normal processing phase of checkpointing
                    rmwInfo.SetRecordInfoAddress(ref srcRecordInfo);
                    ref Value recordValue = ref stackCtx.recSrc.GetValue();

                    if (srcRecordInfo.Tombstone)
                    {
                        if (!this.EnableRevivification)
                            goto CreateNewRecord;

                        // Try in-place revivification of the record.
                        if (!this.LockTable.IsEnabled && !srcRecordInfo.TrySeal(invalidate: false))
                            return OperationStatus.RETRY_NOW;
                        bool ok = true;
                        try
                        {
                            if (srcRecordInfo.Tombstone)
                            {
                                srcRecordInfo.Tombstone = false;

                                if (IsFixedLengthReviv)
                                    rmwInfo.UsedValueLength = rmwInfo.FullValueLength = FixedLengthStruct<Value>.Length;
                                else
                                {
                                    var recordLengths = GetRecordLengths(stackCtx.recSrc.PhysicalAddress, ref recordValue, ref srcRecordInfo);
                                    rmwInfo.FullValueLength = recordLengths.fullValueLength;

                                    // RMW uses GetInitialRecordSize because it has only the initial Input, not a Value
                                    var (requiredSize, _, _) = hlog.GetInitialRecordSize(ref key, ref input, fasterSession);
                                    (ok, rmwInfo.UsedValueLength) = TryReinitializeTombstonedValue<Input, Output, Context, FasterSession>(fasterSession, 
                                            ref srcRecordInfo, ref key, ref recordValue, requiredSize, recordLengths);
                                }

                                if (ok && fasterSession.InitialUpdater(ref key, ref input, ref recordValue, ref output, ref rmwInfo))
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
                            if (ok)
                                SetExtraValueLength(ref recordValue, ref srcRecordInfo, rmwInfo.UsedValueLength, rmwInfo.FullValueLength);
                            else
                                SetTombstoneAndExtraValueLength(ref recordValue, ref srcRecordInfo, rmwInfo.FullValueLength);    // Restore tombstone and ensure default value on inability to update in place
                            srcRecordInfo.Unseal(makeValid: false);
                        }
                        goto CreateNewRecord;
                    }

                    // rmwInfo's lengths are filled in and GetValueLengths and SetLength are called inside InPlaceUpdater, in the ephemeral lock.
                    if (fasterSession.InPlaceUpdater(stackCtx.recSrc.PhysicalAddress, ref key, ref input, ref recordValue, ref output, ref rmwInfo, out status, out stackCtx.recSrc.ephemeralLockResult)
                        || (rmwInfo.Action == RMWAction.ExpireAndStop))
                    {
                        this.MarkPage(stackCtx.recSrc.LogicalAddress, fasterSession.Ctx);

                        // ExpireAndStop means to override default Delete handling (which is to go to InitialUpdater) by leaving the tombstoned record as current.
                        // Our IFasterSession.InPlaceUpdater implementation has already reinitialized-in-place or set Tombstone as appropriate and marked the record.
                        pendingContext.recordInfo = srcRecordInfo;
                        pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                        goto LatchRelease;
                    }

                    // Note: stackCtx.recSrc.ephemeralLockResult == Failed was already handled by 'out status' above
                    if (OperationStatusUtils.BasicOpCode(status) != OperationStatus.SUCCESS)
                        goto LatchRelease;

                    // InPlaceUpdater failed (e.g. insufficient space, another thread set Tombstone, etc). Use this record as the CopyUpdater source.
                    goto CreateNewRecord;
                }
                else if (stackCtx.recSrc.LogicalAddress >= hlog.SafeReadOnlyAddress && !stackCtx.recSrc.GetInfo().Tombstone && latchDestination == LatchDestination.NormalProcessing)
                {
                    // Fuzzy Region: Must retry after epoch refresh, due to lost-update anomaly
                    status = OperationStatus.RETRY_LATER;
                    goto LatchRelease;
                }
                else if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
                {
                    // Safe Read-Only Region: CopyUpdate to create a record in the mutable region
                    goto CreateNewRecord;
                }
                else if (stackCtx.recSrc.LogicalAddress >= hlog.BeginAddress)
                {
                    if (hlog.IsNullDevice)
                        goto CreateNewRecord;

                    // Disk Region: Need to issue async io requests. Locking will be checked on pending completion.
                    status = OperationStatus.RECORD_ON_DISK;
                    latchDestination = LatchDestination.CreatePendingContext;
                    goto CreatePendingContext;
                }
                else
                {
                    // No record exists - create new record.
                    Debug.Assert(!fasterSession.IsManualLocking || LockTable.IsLockedExclusive(ref key, ref stackCtx.hei), "A Lockable-session RMW() of an on-disk or non-existent key requires a LockTable lock");
                    goto CreateNewRecord;
                }
            #endregion Address and source record checks

            #region Create new record
            CreateNewRecord:
                if (latchDestination != LatchDestination.CreatePendingContext)
                {
                    Value tempValue = default;
                    ref var value = ref (stackCtx.recSrc.HasInMemorySrc ? ref stackCtx.recSrc.GetValue() : ref tempValue);

                    // Here, the input* data for 'doingCU' is the same as recSrc.
                    status = CreateNewRecordRMW(ref key, ref input, ref value, ref output, ref pendingContext, fasterSession, ref stackCtx, ref srcRecordInfo,
                                                doingCU: stackCtx.recSrc.HasInMemorySrc && !srcRecordInfo.Tombstone);
                    if (!OperationStatusUtils.IsAppend(status))
                    {
                        // OperationStatus.SUCCESS is OK here; it means NeedCopyUpdate or NeedInitialUpdate returned false
                        if (status == OperationStatus.ALLOCATE_FAILED && pendingContext.IsAsync || status == OperationStatus.RECORD_ON_DISK)
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
            Debug.Assert(latchDestination == LatchDestination.CreatePendingContext, $"RMW CreatePendingContext encountered latchDest == {latchDestination}");
            {
                pendingContext.type = OperationType.RMW;
                if (pendingContext.key == default)
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

        #region Latch release
        LatchRelease:
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
            #endregion

            return status;
        }

        private LatchDestination CheckCPRConsistencyRMW(Phase phase, ref OperationStackContext<Key, Value> stackCtx, ref OperationStatus status, ref LatchOperation latchOperation)
        {
            if (!this.DoTransientLocking)
                return AcquireCPRLatchRMW(phase, ref stackCtx, ref status, ref latchOperation);

            // This is AcquireCPRLatchRMW without the bucket latching, since we already have a latch on either the bucket or the recordInfo.
            // See additional comments in AcquireCPRLatchRMW.

            switch (phase)
            {
                case Phase.PREPARE: // Thread is in V
                    if (!IsEntryVersionNew(ref stackCtx.hei.entry))
                        break; // Normal Processing; thread is in V, record is in V

                    status = OperationStatus.CPR_SHIFT_DETECTED;
                    return LatchDestination.Retry;  // Pivot Thread for retry (do not operate on v+1 record when thread is in V)

                case Phase.IN_PROGRESS: // Thread is in v+1
                case Phase.WAIT_INDEX_CHECKPOINT:
                case Phase.WAIT_FLUSH:
                    if (IsRecordVersionNew(stackCtx.recSrc.LogicalAddress))
                        break;      // Normal Processing; V+1 thread encountered a record in V+1

                    if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
                        return LatchDestination.CreateNewRecord;    // Record is in memory so force creation of a (V+1) record
                    break;  // Normal Processing; the record is below HeadAddress so the operation will go pending

                default:
                    break;
            }
            return LatchDestination.NormalProcessing;
        }

        private LatchDestination AcquireCPRLatchRMW(Phase phase, ref OperationStackContext<Key, Value> stackCtx, ref OperationStatus status, ref LatchOperation latchOperation)
        {
            // The idea of CPR is that if a thread in version V tries to perform an operation and notices a record in V+1, it needs to back off and run CPR_SHIFT_DETECTED.
            // Similarly, a V+1 thread cannot update a V record; it needs to do a read-copy-update (or upsert at tail) instead of an in-place update.
            //  1. V threads take shared lock on bucket
            //  2. V+1 threads take exclusive lock on bucket, refreshing until they can
            //  3. If V thread cannot take shared lock, that means the system is in V+1 so we can immediately refresh and go to V+1 (do CPR_SHIFT_DETECTED)
            //  4. If V thread manages to get shared lock, but encounters a V+1 record, it knows the system is in V+1 so it will do CPR_SHIFT_DETECTED

            switch (phase)
            {
                case Phase.PREPARE: // Thread is in V
                    if (HashBucket.TryAcquireSharedLatch(ref stackCtx.hei))
                    {
                        // Set to release shared latch (default)
                        latchOperation = LatchOperation.Shared;

                        // Here (and in InternalRead, AcquireLatchUpsert, and AcquireLatchDelete) we still check the tail record of the bucket (entry.Address)
                        // rather than the traced record (logicalAddress), because allowing in-place updates for version V when the bucket has arrived at V+1 may have
                        // complications we haven't investigated yet. This is safer but potentially unnecessary, and this case is so rare that the potential
                        // inefficiency is not a concern.
                        if (IsEntryVersionNew(ref stackCtx.hei.entry))
                        {
                            status = OperationStatus.CPR_SHIFT_DETECTED;
                            return LatchDestination.Retry;  // Pivot Thread for retry (do not operate on v+1 record when thread is in V)
                        }
                        break; // Normal Processing; thread is in V, record is in V
                    }

                    // Could not acquire Shared latch; system must be in V+1 (or we have too many shared latches).
                    status = OperationStatus.CPR_SHIFT_DETECTED;
                    return LatchDestination.Retry;  // Pivot Thread for retry

                case Phase.IN_PROGRESS: // Thread is in v+1
                    if (IsRecordVersionNew(stackCtx.recSrc.LogicalAddress))
                        break;      // Normal Processing; V+1 thread encountered a record in V+1

                    if (HashBucket.TryAcquireExclusiveLatch(ref stackCtx.hei))
                    {
                        // Set to release exclusive latch (default)
                        latchOperation = LatchOperation.Exclusive;
                        if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
                            return LatchDestination.CreateNewRecord;    // Record is in memory so force creation of a (V+1) record
                        break; // Normal Processing; the record is below HeadAddress so the operation will go pending
                    }

                    // Could not acquire exclusive latch; likely a conflict on the bucket.
                    status = OperationStatus.RETRY_LATER;
                    return LatchDestination.Retry;  // Refresh and retry

                case Phase.WAIT_INDEX_CHECKPOINT:   // Thread is in V+1
                case Phase.WAIT_FLUSH:
                    if (IsRecordVersionNew(stackCtx.recSrc.LogicalAddress))
                        break;      // Normal Processing; V+1 thread encountered a record in V+1

                    if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
                        return LatchDestination.CreateNewRecord; // Record is in memory so force creation of a (V+1) record
                    break;  // Normal Processing; the record is below HeadAddress so the operation will go pending

                default:
                    break;
            }
            return LatchDestination.NormalProcessing;
        }

        /// <summary>
        /// Create a new record for RMW
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <typeparam name="FasterSession"></typeparam>
        /// <param name="key">The record Key</param>
        /// <param name="input">Input to the operation</param>
        /// <param name="value">Old value</param>
        /// <param name="output">The result of IFunctions.SingleWriter</param>
        /// <param name="pendingContext">Information about the operation context</param>
        /// <param name="fasterSession">The current session</param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions. If called from pending IO,
        ///     this is populated from the data read from disk.</param>
        /// <param name="srcRecordInfo">If <paramref name="stackCtx"/>.<see cref="RecordSource{Key, Value}.HasInMemorySrc"/>,
        ///     this is the <see cref="RecordInfo"/> for <see cref="RecordSource{Key, Value}.LogicalAddress"/>. Otherwise, if called from pending IO,
        ///     this is the <see cref="RecordInfo"/> read from disk. If neither of these, it is a default <see cref="RecordInfo"/>.</param>
        /// <param name="doingCU">Whether we are doing a CopyUpdate, either from in-memory or pending IO</param>
        /// <returns></returns>
        private OperationStatus CreateNewRecordRMW<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Value value, ref Output output,
                                                                                          ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession,
                                                                                          ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo, bool doingCU)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            bool forExpiration = false;

        RetryNow:

            RMWInfo rmwInfo = new()
            {
                Version = fasterSession.Ctx.version,
                SessionID = fasterSession.Ctx.sessionID,
                Address = doingCU && !stackCtx.recSrc.HasReadCacheSrc ? stackCtx.recSrc.LogicalAddress : Constants.kInvalidAddress,
                KeyHash = stackCtx.hei.hash
            };

            // Perform Need*
            if (doingCU)
            {
                rmwInfo.SetRecordInfoAddress(ref srcRecordInfo);
                if (!fasterSession.NeedCopyUpdate(ref key, ref input, ref value, ref output, ref rmwInfo))
                {
                    if (rmwInfo.Action == RMWAction.CancelOperation)
                        return OperationStatus.CANCELED;
                    else if (rmwInfo.Action == RMWAction.ExpireAndResume)
                    {
                        doingCU = false;
                        forExpiration = true;
                    }
                    else
                        return OperationStatus.SUCCESS;
                }
            }

            if (!doingCU)
            {
                rmwInfo.ClearRecordInfoAddress();   // There is no existing record
                if (!fasterSession.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo))
                    return rmwInfo.Action == RMWAction.CancelOperation ? OperationStatus.CANCELED : OperationStatus.NOTFOUND;
            }

            // Allocate and initialize the new record
            var (actualSize, allocatedSize, keySize) = doingCU ?
                stackCtx.recSrc.Log.GetCopyDestinationRecordSize(ref key, ref input, ref value, ref srcRecordInfo, fasterSession) :
                hlog.GetInitialRecordSize(ref key, ref input, fasterSession);

            if (!TryAllocateRecord(fasterSession, ref pendingContext, ref stackCtx, actualSize, ref allocatedSize, keySize, recycle: true,
                    out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status))
                return status;

            ref RecordInfo newRecordInfo = ref WriteNewRecordInfo(ref key, hlog, newPhysicalAddress, inNewVersion: fasterSession.Ctx.InNewVersion, tombstone: false, stackCtx.recSrc.LatestLogicalAddress);

            stackCtx.SetNewRecord(newLogicalAddress);
            rmwInfo.Address = newLogicalAddress;
            rmwInfo.SetRecordInfoAddress(ref newRecordInfo);

            // Populate the new record
            ref Value newRecordValue = ref hlog.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize);
            (rmwInfo.UsedValueLength, rmwInfo.FullValueLength) = GetNewValueLengths(actualSize, allocatedSize, newPhysicalAddress, ref newRecordValue);

            if (!doingCU)
            {
                if (fasterSession.InitialUpdater(ref key, ref input, ref newRecordValue, ref output, ref rmwInfo))
                {
                    SetExtraValueLength(ref newRecordValue, ref newRecordInfo, rmwInfo.UsedValueLength, rmwInfo.FullValueLength);
                    status = forExpiration
                        ? OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord | StatusCode.Expired)
                        : OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord);
                }
                else
                {
                    if (rmwInfo.Action == RMWAction.CancelOperation)
                        return OperationStatus.CANCELED;
                    return OperationStatus.NOTFOUND | (forExpiration ? OperationStatus.EXPIRED : OperationStatus.NOTFOUND);
                }
            }
            else
            {
                if (fasterSession.CopyUpdater(ref key, ref input, ref value, ref newRecordValue, ref output, ref rmwInfo))
                {
                    SetExtraValueLength(ref newRecordValue, ref newRecordInfo, rmwInfo.UsedValueLength, rmwInfo.FullValueLength);
                    status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.CopyUpdatedRecord);
                    goto DoCAS;
                }
                if (rmwInfo.Action == RMWAction.CancelOperation)
                {
                    // Save allocation for revivification (not retry, because this is canceling of the current operation), or abandon it if that fails.
                    if (this.UseFreeRecordPool && this.FreeRecordPool.TryAdd(newLogicalAddress, newPhysicalAddress, allocatedSize))
                        stackCtx.ClearNewRecord();
                    else
                        stackCtx.SetNewRecordInvalid(ref newRecordInfo);
                    return OperationStatus.CANCELED;
                }
                if (rmwInfo.Action == RMWAction.ExpireAndStop)
                {
                    newRecordInfo.Tombstone = true;
                    status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.CreatedRecord | StatusCode.Expired | StatusCode.Expired);
                    goto DoCAS;
                }
                else if (rmwInfo.Action == RMWAction.ExpireAndResume)
                {
                    doingCU = false;
                    forExpiration = true;
                        
                    if (!ReinitializeExpiredRecord<Input, Output, Context, FasterSession>(ref key, ref input, ref newRecordValue, ref output, ref newRecordInfo,
                                            ref rmwInfo, newLogicalAddress, fasterSession, isIpu: false, out status))
                    {
                        // An IPU was not (or could not) be done. Cancel if requested, else invalidate the allocated record and retry.
                        if (status == OperationStatus.CANCELED)
                            return status;

                        // Save allocation for revivification (not retry, because this may have been false because the record was too small), or abandon it if that fails.
                        if (this.UseFreeRecordPool && this.FreeRecordPool.TryAdd(newLogicalAddress, newPhysicalAddress, allocatedSize))
                            stackCtx.ClearNewRecord();
                        else
                            stackCtx.SetNewRecordInvalid(ref newRecordInfo);
                        goto RetryNow;
                    }
                    goto DoCAS;
                }
                else
                    return OperationStatus.SUCCESS | (forExpiration ? OperationStatus.EXPIRED : OperationStatus.SUCCESS);
            }

        DoCAS:
            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            bool success = CASRecordIntoChain(ref key, ref stackCtx, newLogicalAddress, ref newRecordInfo);
            if (success)
            {
                PostCopyToTail(ref key, ref stackCtx, ref srcRecordInfo);

                // If IU, status will be NOTFOUND; return that.
                if (!doingCU)
                {
                    // If IU, status will be NOTFOUND. ReinitializeExpiredRecord has many paths but is straightforward so no need to assert here.
                    Debug.Assert(forExpiration || OperationStatus.NOTFOUND == OperationStatusUtils.BasicOpCode(status), $"Expected NOTFOUND but was {status}");
                    fasterSession.PostInitialUpdater(ref key, ref input, ref hlog.GetValue(newPhysicalAddress), ref output, ref rmwInfo);
                }
                else
                {
                    // Else it was a CopyUpdater so call PCU
                    fasterSession.PostCopyUpdater(ref key, ref input, ref value, ref hlog.GetValue(newPhysicalAddress), ref output, ref rmwInfo);

                    // Success should always Seal the old record.
                    srcRecordInfo.UnlockExclusiveAndSeal();
                }
                stackCtx.ClearNewRecord();

                pendingContext.recordInfo = newRecordInfo;
                pendingContext.logicalAddress = newLogicalAddress;
                return status;
            }

            // CAS failed
            stackCtx.SetNewRecordInvalid(ref newRecordInfo);
            ref Value insertedValue = ref hlog.GetValue(newPhysicalAddress);
            ref Key insertedKey = ref hlog.GetKey(newPhysicalAddress);
            if (!doingCU)
                fasterSession.DisposeInitialUpdater(ref insertedKey, ref input, ref insertedValue, ref output, ref rmwInfo);
            else
                fasterSession.DisposeCopyUpdater(ref insertedKey, ref input, ref value, ref insertedValue, ref output, ref rmwInfo);

            SetExtraValueLength(ref newRecordValue, ref newRecordInfo, rmwInfo.UsedValueLength, rmwInfo.FullValueLength);
            SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
            return OperationStatus.RETRY_NOW;   // CAS failure does not require epoch refresh
        }

        internal bool ReinitializeExpiredRecord<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo,
                                                                                       long logicalAddress, FasterSession fasterSession, bool isIpu, out OperationStatus status)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // This is called for InPlaceUpdater or CopyUpdater only; CopyUpdater however does not copy an expired record, so we return CreatedRecord.
            var advancedStatusCode = isIpu ? StatusCode.InPlaceUpdatedRecord : StatusCode.CreatedRecord;
            advancedStatusCode |= StatusCode.Expired;
            if (!fasterSession.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo))
            {
                if (rmwInfo.Action == RMWAction.CancelOperation)
                {
                    status = OperationStatus.CANCELED;
                    return false;
                }

                // Expiration with no insertion.
                recordInfo.Tombstone = true;
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, advancedStatusCode);
                return true;
            }

            // Try to reinitialize in place
            (var currentSize, _, _) = hlog.GetRecordSize(ref key, ref value);
            (var requiredSize, _, _) = hlog.GetInitialRecordSize(ref key, ref input, fasterSession);

            if (currentSize >= requiredSize)
            {
                if (fasterSession.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo))
                {
                    // If IPU path, we need to complete PostInitialUpdater as well
                    if (isIpu)
                        fasterSession.PostInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);

                    status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, advancedStatusCode);
                    return true;
                }
                else
                {
                    if (rmwInfo.Action == RMWAction.CancelOperation)
                    {
                        status = OperationStatus.CANCELED;
                        return false;
                    }
                    else
                    {
                        // Expiration with no insertion.
                        recordInfo.Tombstone = true;
                        status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, advancedStatusCode);
                        return true;
                    }
                }
            }

            // Reinitialization in place was not possible. InternalRMW will do the following based on who called this:
            //  IPU: move to the NIU->allocate->IU path
            //  CU: caller invalidates allocation, retries operation as NIU->allocate->IU
            status = OperationStatus.SUCCESS;
            return false;
        }
    }
}
