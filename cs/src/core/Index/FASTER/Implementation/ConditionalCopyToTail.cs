// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        /// <summary>
        /// Copy a record to the tail of the log after caller has verified it does not exist within a specified range.
        /// </summary>
        /// <param name="fasterSession">Callback functions.</param>
        /// <param name="pendingContext">pending context created when the operation goes pending.</param>
        /// <param name="key">key of the record.</param>
        /// <param name="input">input passed through.</param>
        /// <param name="value">the value to insert</param>
        /// <param name="output">Location to store output computed from input and value.</param>
        /// <param name="userContext">user context corresponding to operation used during completion callback.</param>
        /// <param name="lsn">Operation serial number</param>
        /// <param name="stackCtx">Contains information about the call context, record metadata, and so on</param>
        /// <param name="writeReason">The reason the CopyToTail is being done</param>
        /// <param name="wantIO">Whether to do IO if the search must go below HeadAddress. ReadFromImmutable, for example,
        ///     is just an optimization to avoid future IOs, so if we need an IO here we just defer them to the next Read().</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private OperationStatus ConditionalCopyToTail<Input, Output, Context, FasterSession>(FasterSession fasterSession,
                ref PendingContext<Input, Output, Context> pendingContext,
                ref Key key, ref Input input, ref Value value, ref Output output, ref Context userContext, long lsn,
                ref OperationStackContext<Key, Value> stackCtx, WriteReason writeReason, bool wantIO = true)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            bool callerHasLock = stackCtx.recSrc.HasTransientLock;

            // We are called by one of ReadFromImmutable, CompactionConditionalCopyToTail, or ContinuePendingConditionalCopyToTail;
            // these have already searched to see if the record is present above minAddress, and stackCtx is set up for the first try.
            // minAddress is the stackCtx.recSrc.LatestLogicalAddress; by the time we get here, any IO below that has been done due to
            // PrepareConditionalCopyToTailIO, which then went to ContinuePendingConditionalCopyToTail, which evaluated whether the
            // record was found at that level.
            while (true)
            {
                // ConditionalCopyToTail is different in regard to locking from the usual procedures, in that if we find a source record we don't lock--we exit with success.
                // So we only do LockTable-based locking and only when we are about to insert at the tail.
                if (callerHasLock || TryTransientSLock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx, out OperationStatus status))
                {
                    try
                    {
                        RecordInfo dummyRecordInfo = default;   // TryCopyToTail only needs this for readcache record invalidation.
                        status = TryCopyToTail(ref pendingContext, ref key, ref input, ref value, ref output, ref stackCtx, ref dummyRecordInfo, fasterSession, writeReason);
                    }
                    finally
                    {
                        stackCtx.HandleNewRecordOnException(this);
                        if (!callerHasLock)
                            TransientSUnlock<Input, Output, Context, FasterSession>(fasterSession, ref key, ref stackCtx);
                    }
                }

                // If we're here we failed TryCopyToTail, probably a failed CAS due to another record insertion.
                if (!HandleImmediateRetryStatus(status, fasterSession, ref pendingContext))
                    return status;

                // HandleImmediateRetryStatus may have been refreshed the epoch which means HeadAddress etc. may have changed. Re-traverse from the tail to the highest
                // point we just searched (which may have gone below HeadAddress). +1 to LatestLogicalAddress because we have examined that already. Use stackCtx2 to
                // preserve stacKCtx, both for retrying the Insert if needed and for preserving the caller's lock status, etc.
                var minAddress = stackCtx.recSrc.LatestLogicalAddress + 1;
                OperationStackContext<Key, Value> stackCtx2 = new(stackCtx.hei.hash);
                if (TryFindRecordInMainLogForConditionalCopyToTail(ref key, ref stackCtx2, minAddress, out bool needIO))
                    return OperationStatus.SUCCESS;

                // Issue IO if necessary and desired (for ReadFromImmutable, it isn't; just exit in that case), else loop back up and retry the insert.
                if (!wantIO)
                {
                    // Caller (e.g. ReadFromImmutable) called this to keep read-hot records in memory. That's already failed, so give up and we'll read it when we have to.
                    if (stackCtx.recSrc.LogicalAddress < hlog.HeadAddress)
                        return OperationStatus.SUCCESS;
                }
                else if (needIO)
                    return PrepareIOForConditionalCopyToTail(fasterSession, ref pendingContext, ref key, ref input, ref value, ref output, ref userContext, lsn,
                                                      ref stackCtx2, minAddress, WriteReason.Compaction);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status CompactionConditionalCopyToTail<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref Key key, ref Input input, ref Value value, 
                ref Output output, long minAddress)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "This is called only from Compaction so the epoch should be protected");
            PendingContext<Input, Output, Context> pendingContext = new();

            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));
            if (TryFindRecordInMainLogForConditionalCopyToTail(ref key, ref stackCtx, minAddress, out bool needIO))
                return Status.CreateFound();

            Context userContext = default;
            OperationStatus status;
            if (needIO)
                status = PrepareIOForConditionalCopyToTail(fasterSession, ref pendingContext, ref key, ref input, ref value, ref output, ref userContext, 0L,
                                                    ref stackCtx, minAddress, WriteReason.Compaction);
            else
                status = ConditionalCopyToTail(fasterSession, ref pendingContext, ref key, ref input, ref value, ref output, ref userContext, 0L, ref stackCtx, WriteReason.Compaction);
            return HandleOperationStatus(fasterSession.Ctx, ref pendingContext, status, out _);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private OperationStatus PrepareIOForConditionalCopyToTail<Input, Output, Context, FasterSession>(FasterSession fasterSession, ref PendingContext<Input, Output, Context> pendingContext,
                                        ref Key key, ref Input input, ref Value value, ref Output output, ref Context userContext, long lsn,
                                        ref OperationStackContext<Key, Value> stackCtx, long minAddress, WriteReason writeReason)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            pendingContext.type = OperationType.CONDITIONAL_INSERT;
            pendingContext.minAddress = minAddress;
            pendingContext.writeReason = writeReason;
            pendingContext.InitialEntryAddress = Constants.kInvalidAddress;
            pendingContext.InitialLatestLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;

            if (!pendingContext.NoKey && pendingContext.key == default)    // If this is true, we don't have a valid key
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

            return OperationStatus.RECORD_ON_DISK;
        }
    }
}
