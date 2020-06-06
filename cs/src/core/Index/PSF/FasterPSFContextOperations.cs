// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

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
        internal Status ContextPsfReadAddress(ref PSFReadArgs<Key, Value> psfArgs, long serialNo,
                                         FasterExecutionContext sessionCtx)
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
        internal Status ContextPsfUpdate<TProviderData, TRecordId>(ref GroupKeysPair groupKeysPair, ref Value value, ref Input input, long serialNo,
                                                                   FasterExecutionContext sessionCtx,
                                                                   PSFChangeTracker<TProviderData, TRecordId> changeTracker)
            where TRecordId : struct
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
                this.chainPost.SetRecordId(ref value, changeTracker.AfterRecordId);
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
        internal Status ContextPsfDelete<TProviderData, TRecordId>(ref Key key, ref Value value, ref Input input, long serialNo,
                                                                   FasterExecutionContext sessionCtx,
                                                                   PSFChangeTracker<TProviderData, TRecordId> changeTracker)
            where TRecordId : struct
        {
            var pcontext = default(PendingContext);

            // TODO check cache first:
            // var internalStatus = this.PsfInternalDeleteByCache(ref key, ref input, 
            //                                                    ref pcontext, sessionCtx, serialNo));
            // if (internalStatus == OperationStatus.NOTFOUND)
            //      ... do below ...

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
