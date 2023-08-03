// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        /// <summary>
        /// Copy a record from the disk to the read cache.
        /// </summary>
        /// <param name="pendingContext"></param>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="recordValue"></param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="fasterSession"></param>
        /// <returns>True if copied to readcache, else false; readcache is "best effort", and we don't fail the read process, or slow it down by retrying.
        /// </returns>
        internal bool TryCopyToReadCache<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref PendingContext<Input, Output, Context> pendingContext,
                                        ref Key key, ref Input input, ref Value recordValue, ref OperationStackContext<Key, Value> stackCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            #region Create new copy in mutable region
            var (actualSize, allocatedSize) = hlog.GetRecordSize(ref key, ref recordValue);

            #region Allocate new record and call SingleWriter

            if (!TryAllocateRecordReadCache(ref pendingContext, ref stackCtx, allocatedSize, out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status))
                return false;
            ref var newRecordInfo = ref WriteNewRecordInfo(ref key, readcache, newPhysicalAddress, inNewVersion: false, tombstone: false, stackCtx.hei.Address, RecycleMode.None);
            stackCtx.SetNewRecord(newLogicalAddress | Constants.kReadCacheBitMask);

            UpsertInfo upsertInfo = new()
            {
                Version = fasterSession.Ctx.version,
                SessionID = fasterSession.Ctx.sessionID,
                Address = Constants.kInvalidAddress,        // We do not expose readcache addresses
                KeyHash = stackCtx.hei.hash,
            };
            upsertInfo.SetRecordInfoAddress(ref newRecordInfo);

            Output output = default;
            if (!fasterSession.SingleWriter(ref key, ref input, ref recordValue, ref readcache.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize),
                                            ref output, ref upsertInfo, WriteReason.CopyToReadCache))
            {
                stackCtx.SetNewRecordInvalid(ref newRecordInfo);
                return false;
            }

            #endregion Allocate new record and call SingleWriter

            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            // It is possible that we will successfully CAS but subsequently fail validation.
            OperationStatus failStatus = OperationStatus.RETRY_NOW;     // Default to CAS-failed status, which does not require an epoch refresh

            // ReadCache entries are CAS'd in as the first entry in the hash chain.
            var success = stackCtx.hei.TryCAS(newLogicalAddress | Constants.kReadCacheBitMask);
            var casSuccess = success;

            if (success && stackCtx.recSrc.LowestReadCacheLogicalAddress != Constants.kInvalidAddress)
            {
                // If someone added a main-log entry for this key from an update or CTT while we were inserting the new readcache record, then the new
                // readcache record is obsolete and must be Invalidated. (If LowestReadCacheLogicalAddress == kInvalidAddress, then the CAS would have
                // failed in this case.) If this was the first readcache record in the chain, then once we CAS'd it in someone could have spliced into
                // it, but then that splice will call ReadCacheCheckTailAfterSplice and invalidate it if it's the same key.
                success = EnsureNoNewMainLogRecordWasSpliced(ref key, stackCtx.recSrc, pendingContext.InitialLatestLogicalAddress, ref failStatus);
            }

            if (!success)
            {
                stackCtx.SetNewRecordInvalid(ref newRecordInfo);

                if (!casSuccess)
                {
                    // Let user dispose similar to a deleted record, and save for retry, *only* if CAS failed; otherwise we must preserve it in the chain.
                    fasterSession.DisposeSingleWriter(ref readcache.GetKey(newPhysicalAddress), ref input, ref recordValue, ref readcache.GetValue(newPhysicalAddress),
                                                      ref output, ref upsertInfo, WriteReason.CopyToReadCache);
                    newRecordInfo.PreviousAddress = Constants.kTempInvalidAddress;     // Necessary for ReadCacheEvict, but cannot be kInvalidAddress or we have recordInfo.IsNull
                }
                return false;
            }

            // Success.
            pendingContext.recordInfo = newRecordInfo;
            pendingContext.logicalAddress = upsertInfo.Address;
            fasterSession.PostSingleWriter(ref key, ref input, ref recordValue, ref readcache.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize), ref output,
                                    ref upsertInfo, WriteReason.CopyToReadCache);
            stackCtx.ClearNewRecord();
            return true;
            #endregion
        }
    }
}
