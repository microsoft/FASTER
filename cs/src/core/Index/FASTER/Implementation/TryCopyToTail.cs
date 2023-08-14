// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
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
        internal OperationStatus TryCopyToTail<Input, Output, Context, FasterSession>(ref PendingContext<Input, Output, Context> pendingContext,
                                    ref Key key, ref Input input, ref Value value, ref Output output, ref OperationStackContext<Key, Value> stackCtx,
                                    ref RecordInfo srcRecordInfo, FasterSession fasterSession, WriteReason reason)
        where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var (actualSize, allocatedSize, keySize) = hlog.GetRecordSize(ref key, ref value);
            if (!TryAllocateRecord(fasterSession, ref pendingContext, ref stackCtx, actualSize, ref allocatedSize, keySize, recycle: true, 
                    out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status))
                return status;
            ref var newRecordInfo = ref WriteNewRecordInfo(ref key, hlog, newPhysicalAddress, inNewVersion: fasterSession.Ctx.InNewVersion, tombstone: false, stackCtx.recSrc.LatestLogicalAddress);
            stackCtx.SetNewRecord(newLogicalAddress);

            UpsertInfo upsertInfo = new()
            {
                Version = fasterSession.Ctx.version,
                SessionID = fasterSession.Ctx.sessionID,
                Address = newLogicalAddress,
                KeyHash = stackCtx.hei.hash,
            };
            upsertInfo.SetRecordInfoAddress(ref newRecordInfo);

            ref Value newRecordValue = ref hlog.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize);
            (upsertInfo.UsedValueLength, upsertInfo.FullValueLength) = GetNewValueLengths(actualSize, allocatedSize, newPhysicalAddress, ref newRecordValue);

            if (!fasterSession.SingleWriter(ref key, ref input, ref value, ref hlog.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize),
                                            ref output, ref upsertInfo, reason))
            {
                // Save allocation for revivification (not retry, because we won't retry here), or abandon it if that fails.
                if (this.UseFreeRecordPool && this.FreeRecordPool.TryAdd(newLogicalAddress, newPhysicalAddress, allocatedSize))
                    stackCtx.ClearNewRecord();
                else
                    stackCtx.SetNewRecordInvalid(ref newRecordInfo);
                return (upsertInfo.Action == UpsertAction.CancelOperation) ? OperationStatus.CANCELED : OperationStatus.SUCCESS;
            }
            SetExtraValueLength(ref newRecordValue, ref srcRecordInfo, upsertInfo.UsedValueLength, upsertInfo.FullValueLength);

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
            { 
                newRecordInfo.UnsealAndValidate();
                PostCopyToTail(ref key, ref stackCtx, ref srcRecordInfo, pendingContext.InitialEntryAddress);
            } 
            else
            {
                stackCtx.SetNewRecordInvalid(ref newRecordInfo);

                // CAS failed, so let the user dispose similar to a deleted record, and save for retry.
                fasterSession.DisposeSingleWriter(ref hlog.GetKey(newPhysicalAddress), ref input, ref value, ref hlog.GetValue(newPhysicalAddress),
                                                    ref output, ref upsertInfo, reason);
                SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
                return failStatus;
            }

            // Success, and any read locks have been transferred.
            pendingContext.recordInfo = newRecordInfo;
            pendingContext.logicalAddress = upsertInfo.Address;
            fasterSession.PostSingleWriter(ref key, ref input, ref value, ref hlog.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize), ref output,
                                           ref upsertInfo, reason);
            stackCtx.ClearNewRecord();
            return OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.Found | StatusCode.CopiedRecord);
        }
    }
}
