// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase,
        IFasterKV<Key, Value, Input, Output, Context, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextPsfReadKey(ref Key key, ref PSFReadArgs<Key, Value> psfArgs, long serialNo,
                                         FasterExecutionContext sessionCtx)
        {
            var pcontext = default(PendingContext);
            var internalStatus = this.PsfInternalReadKey(ref key, ref psfArgs, ref pcontext, sessionCtx, serialNo);
            var status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, pcontext, internalStatus);

            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<ReadAsyncResult> ContextPsfReadKeyAsync(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                                        ref Key key, ref PSFReadArgs<Key, Value> psfArgs, long serialNo, FasterExecutionContext sessionCtx,
                                        PSFQuerySettings querySettings)
        {
            return PsfReadAsync(clientSession, isKey: true, ref key, ref psfArgs, serialNo, sessionCtx, querySettings);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextPsfReadAddress(ref PSFReadArgs<Key, Value> psfArgs, long serialNo, FasterExecutionContext sessionCtx)
        {
            var pcontext = default(PendingContext);
            var internalStatus = this.PsfInternalReadAddress(ref psfArgs, ref pcontext, sessionCtx, serialNo);
            var status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, pcontext, internalStatus);

            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<ReadAsyncResult> ContextPsfReadAddressAsync(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                                        ref PSFReadArgs<Key, Value> psfArgs, long serialNo, FasterExecutionContext sessionCtx,
                                        PSFQuerySettings querySettings)
        {
            var key = default(Key);
            return PsfReadAsync(clientSession, isKey: false, ref key, ref psfArgs, serialNo, sessionCtx, querySettings);
        }

        internal ValueTask<ReadAsyncResult> PsfReadAsync(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, bool isKey,
                                                         ref Key key, ref PSFReadArgs<Key, Value> psfArgs, long serialNo, FasterExecutionContext sessionCtx,
                                                         PSFQuerySettings querySettings)
        {
            var pcontext = default(PendingContext);
            var output = default(Output);
            var nextSerialNum = clientSession.ctx.serialNum + 1;

            if (clientSession.SupportAsync) clientSession.UnsafeResumeThread();
            try
            {
            TryReadAgain:
                var internalStatus = isKey
                    ? this.PsfInternalReadKey(ref key, ref psfArgs, ref pcontext, sessionCtx, serialNo)
                    : this.PsfInternalReadAddress(ref psfArgs, ref pcontext, sessionCtx, serialNo);
                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                {
                    return new ValueTask<ReadAsyncResult>(new ReadAsyncResult((Status)internalStatus, output));
                }

                if (internalStatus == OperationStatus.CPR_SHIFT_DETECTED)
                {
                    SynchronizeEpoch(clientSession.ctx, clientSession.ctx, ref pcontext);
                    goto TryReadAgain;
                }
            }
            finally
            {
                clientSession.ctx.serialNum = nextSerialNum;
                if (clientSession.SupportAsync) clientSession.UnsafeSuspendThread();
            }

            try
            { 
                return SlowReadAsync(this, clientSession, pcontext, querySettings.CancellationToken);
            }
            catch (OperationCanceledException) when (!querySettings.ThrowOnCancellation)
            {
                return default;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextPsfInsert(ref Key key, ref Value value, ref Input input, long serialNo,
                                         FasterExecutionContext sessionCtx)
        {
            var pcontext = default(PendingContext);
            var internalStatus = this.PsfInternalInsert(ref key, ref value, ref input,
                                                      ref pcontext, sessionCtx, serialNo);
            var status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, pcontext, internalStatus);

            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextPsfUpdate<TProviderData>(ref GroupKeysPair groupKeysPair, ref Value value, ref Input input, long serialNo,
                                                                   FasterExecutionContext sessionCtx,
                                                                   PSFChangeTracker<TProviderData, Value> changeTracker)
        {
            var pcontext = default(PendingContext);
            var psfInput = (IPSFInput<Key>)input;

            var groupKeys = groupKeysPair.Before;
            unsafe { psfInput.SetFlags(groupKeys.ResultFlags); }
            psfInput.IsDelete = true;

            var internalStatus = this.PsfInternalInsert(ref groupKeys.GetCompositeKeyRef<Key>(), ref value, ref input,
                                                        ref pcontext, sessionCtx, serialNo);
            Status status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, pcontext, internalStatus);

            sessionCtx.serialNum = serialNo;

            if (status == Status.OK)
            {
                value = changeTracker.AfterRecordId;
                return PsfRcuInsert(groupKeysPair.After, ref value, ref input, ref pcontext, sessionCtx, serialNo + 1);
            }
            return status;
        }

        private Status PsfRcuInsert(GroupKeys groupKeys, ref Value value, ref Input input,
                                    ref PendingContext pcontext, FasterExecutionContext sessionCtx, long serialNo)
        {
            var psfInput = (IPSFInput<Key>)input;
            unsafe { psfInput.SetFlags(groupKeys.ResultFlags); }
            psfInput.IsDelete = false;
            var internalStatus = this.PsfInternalInsert(ref groupKeys.GetCompositeKeyRef<Key>(), ref value, ref input,
                                                        ref pcontext, sessionCtx, serialNo);
            return internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, pcontext, internalStatus);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextPsfDelete<TProviderData>(ref Key key, ref Value value, ref Input input, long serialNo,
                                                                   FasterExecutionContext sessionCtx,
                                                                   PSFChangeTracker<TProviderData, Value> changeTracker)
        {
            var pcontext = default(PendingContext);

            var psfInput = (IPSFInput<Key>)input;
            psfInput.IsDelete = true;
            var internalStatus = this.PsfInternalInsert(ref key, ref value, ref input,
                                                        ref pcontext, sessionCtx, serialNo);
            Status status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, pcontext, internalStatus);

            sessionCtx.serialNum = serialNo;
            return status;
        }
    }
}
