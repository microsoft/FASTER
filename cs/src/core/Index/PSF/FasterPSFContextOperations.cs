// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
        where Key : new()
        where Value : new()
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextPsfReadKey<Input, Output, Context, FasterSession>(ref Key key, ref PSFReadArgs<Key, Value> psfArgs, FasterSession fasterSession,
                                        long serialNo, FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            var internalStatus = this.PsfInternalReadKey(ref key, ref psfArgs, ref pcontext, fasterSession, sessionCtx, serialNo);
            var status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, pcontext, fasterSession, internalStatus);

            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<ReadAsyncResult<Input, Output, Context, Functions>> ContextPsfReadKeyAsync<Input, Output, Context, Functions>(
                                        ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                                        ref Key key, ref PSFReadArgs<Key, Value> psfArgs, long serialNo, FasterExecutionContext<Input, Output, Context> sessionCtx,
                                        PSFQuerySettings querySettings)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            return ContextPsfReadAsync(clientSession, isKey: true, ref key, ref psfArgs, serialNo, sessionCtx, querySettings);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextPsfReadAddress<Input, Output, Context, FasterSession>(ref PSFReadArgs<Key, Value> psfArgs, FasterSession fasterSession,
                                        long serialNo, FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            var internalStatus = this.PsfInternalReadAddress(ref psfArgs, ref pcontext, fasterSession, sessionCtx, serialNo);
            var status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, pcontext, fasterSession, internalStatus);

            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<ReadAsyncResult<Input, Output, Context, Functions>> ContextPsfReadAddressAsync<Input, Output, Context, Functions>(
                                        ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                                        ref PSFReadArgs<Key, Value> psfArgs, long serialNo, FasterExecutionContext<Input, Output, Context> sessionCtx,
                                        PSFQuerySettings querySettings)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            var key = default(Key);
            return ContextPsfReadAsync(clientSession, isKey: false, ref key, ref psfArgs, serialNo, sessionCtx, querySettings);
        }

        internal ValueTask<ReadAsyncResult<Input, Output, Context, Functions>> ContextPsfReadAsync<Input, Output, Context, Functions>(
                                        ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, bool isKey,
                                        ref Key key, ref PSFReadArgs<Key, Value> psfArgs, long serialNo, FasterExecutionContext<Input, Output, Context> sessionCtx,
                                        PSFQuerySettings querySettings)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            var output = default(Output);
            var nextSerialNum = clientSession.ctx.serialNum + 1;

            if (clientSession.SupportAsync) clientSession.UnsafeResumeThread();
            try
            {
            TryReadAgain:
                var internalStatus = isKey
                    ? this.PsfInternalReadKey(ref key, ref psfArgs, ref pcontext, clientSession.FasterSession, sessionCtx, serialNo)
                    : this.PsfInternalReadAddress(ref psfArgs, ref pcontext, clientSession.FasterSession, sessionCtx, serialNo);
                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                {
                    return new ValueTask<ReadAsyncResult<Input, Output, Context, Functions>>(new ReadAsyncResult<Input, Output, Context, Functions>((Status)internalStatus, output));
                }

                if (internalStatus == OperationStatus.CPR_SHIFT_DETECTED)
                {
                    SynchronizeEpoch(clientSession.ctx, clientSession.ctx, ref pcontext, clientSession.FasterSession);
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
        internal Status ContextPsfInsert<Input, Output, Context, FasterSession>(ref Key key, ref Value value, ref Input input, 
                                         FasterSession fasterSession, long serialNo,
                                         FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            var internalStatus = this.PsfInternalInsert(ref key, ref value, ref input,
                                                      ref pcontext, fasterSession, sessionCtx, serialNo);
            var status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, pcontext, fasterSession, internalStatus);

            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextPsfUpdate<Input, Output, Context, FasterSession, TProviderData>(ref GroupKeysPair groupKeysPair, ref Value value, ref Input input, 
                                                                   FasterSession fasterSession, long serialNo,
                                                                   FasterExecutionContext<Input, Output, Context> sessionCtx,
                                                                   PSFChangeTracker<TProviderData, Value> changeTracker)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            var psfInput = (IPSFInput<Key>)input;

            var groupKeys = groupKeysPair.Before;
            unsafe { psfInput.SetFlags(groupKeys.ResultFlags); }
            psfInput.IsDelete = true;

            var internalStatus = this.PsfInternalInsert(ref groupKeys.GetCompositeKeyRef<Key>(), ref value, ref input,
                                                        ref pcontext, fasterSession, sessionCtx, serialNo);
            Status status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, pcontext, fasterSession, internalStatus);

            sessionCtx.serialNum = serialNo;

            if (status == Status.OK)
            {
                value = changeTracker.AfterRecordId;
                return PsfRcuInsert(groupKeysPair.After, ref value, ref input, ref pcontext, fasterSession, sessionCtx, serialNo + 1);
            }
            return status;
        }

        private Status PsfRcuInsert<Input, Output, Context, FasterSession>(GroupKeys groupKeys, ref Value value, ref Input input,
                                    ref PendingContext<Input, Output, Context> pcontext, FasterSession fasterSession, 
                                    FasterExecutionContext<Input, Output, Context> sessionCtx, long serialNo)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var psfInput = (IPSFInput<Key>)input;
            unsafe { psfInput.SetFlags(groupKeys.ResultFlags); }
            psfInput.IsDelete = false;
            var internalStatus = this.PsfInternalInsert(ref groupKeys.GetCompositeKeyRef<Key>(), ref value, ref input,
                                                        ref pcontext, fasterSession, sessionCtx, serialNo);
            return internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, pcontext, fasterSession, internalStatus);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextPsfDelete<Input, Output, Context, FasterSession, TProviderData>(ref Key key, ref Value value, ref Input input, 
                                                                   FasterSession fasterSession, long serialNo,
                                                                   FasterExecutionContext<Input, Output, Context> sessionCtx,
                                                                   PSFChangeTracker<TProviderData, Value> changeTracker)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);

            var psfInput = (IPSFInput<Key>)input;
            psfInput.IsDelete = true;
            var internalStatus = this.PsfInternalInsert(ref key, ref value, ref input, ref pcontext, fasterSession, sessionCtx, serialNo);
            Status status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, pcontext, fasterSession, internalStatus);

            sessionCtx.serialNum = serialNo;
            return status;
        }
    }
}
