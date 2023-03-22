// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private OperationStatus ConditionalInsert<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref Input input, ref Value value, ref Output output,
                long minAddress, ref OperationStackContext<Key, Value> stackCtx, ref PendingContext<Input, Output, Context> pendingContext, WriteReason writeReason)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            // 1. Check for the presence of the key in the in-memory unexplored region of the log (down to recSrc.LatestLogicalAddress).
            //    We will not be here if it was found in-memory below this.
            //      - stackCtx is set by the caller, which has done TryFindRecordInMemory.
            //          - This includes setting LowestReadCacheLogicalAddress (which is necessary to get to the first mainlog record).
            //      - Do not check the readcache here.
            // 2. If it is found, do nothing and exit.
            // 3. Otherwise, if the minAddress is below HeadAddress, we must issue pending IO.
            //      - This may entail going through the Pending IO process.
            // 2. Call TryCopyToTail to insert the record at the tail of the log (this may involve splicing into the readcache/mainlog boundary).
            //      - HandleImmediateRetryStatus handles the relevant retry cases:
            //          - If TryCopyToTail returns RETRY_NOW, it means CAS failed and we should restart at 1.
            //          - If TryCopyToTail returns RETRY_LATER, it means BlockAllocate caused an epoch refresh, and we should restart at 1.
            //      - TryCopyToTail handles invalidating the record in the readcache.

            // ConditionalInsert is different in regard to locking from the usual procedures, in that if we find a source record we don't lock--we exit with success.
            // So we only do LockTable-based locking and only when we are about to insert.

            OperationStatus status;
            RecordInfo dummyRecordInfo = default;   // TryCopyToTail only needs this for readcache record invalidation.
            do
            {
                // First explore the unexplored log regions. If we are here then either ReadFromImmutable, CompactionConditionalInsert, or CompleteConditionalInsert have
                // called us, and they all searched for the record in main log memory, so stackCtx is set up.
                var highestAddress = stackCtx.recSrc.LatestLogicalAddress;
                if (stackCtx.hei.IsReadCache)
                {
                    var rcri = readcache.GetInfo(stackCtx.recSrc.LowestReadCachePhysicalAddress);
                    if (rcri.PreviousAddress > highestAddress)
                        highestAddress = rcri.PreviousAddress;
                }
                else if (stackCtx.hei.IsNotCurrent)
                {
                    highestAddress = stackCtx.hei.CurrentAddress;
                }
                if (highestAddress > stackCtx.recSrc.LatestLogicalAddress && TraceBackForKeyMatch(ref key, highestAddress, stackCtx.recSrc.LatestLogicalAddress + 1, out _, out _))
                    return OperationStatus.SUCCESS;

                status = TryCopyToTail(ref pendingContext, ref key, ref input, ref value, ref output, ref stackCtx, ref dummyRecordInfo, fasterSession, writeReason);
            } while (HandleImmediateRetryStatus(status, fasterSession, ref pendingContext));

            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus CompactionConditionalInsert<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref Input input, ref Value value, ref Output output, long minAddress)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "This is currently only called from Compaction so the epoch should be protected");
            PendingContext<Input, Output, Context> pendingContext = new() { minAddress = minAddress };

            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));
            if (TryFindRecordInMainLogOnly(ref key, ref stackCtx, pendingContext.minAddress))
                return OperationStatus.SUCCESS;
            return ConditionalInsert(fasterSession, ref key, ref input, ref value, ref output, minAddress, ref stackCtx, ref pendingContext, WriteReason.Compaction);
        }
    }
}
