// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status InternalContainsKeyInMemory<Input, Output, Context, FasterSession>(
            ref Key key,
            FasterExecutionContext<Input, Output, Context> sessionCtx,
            FasterSession fasterSession, out long logicalAddress, long fromAddress = -1)
            where FasterSession : IFasterSession
        {
            if (fromAddress < hlog.HeadAddress)
                fromAddress = hlog.HeadAddress;

            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));

            if (sessionCtx.phase != Phase.REST)
                HeavyEnter(stackCtx.hei.hash, sessionCtx, fasterSession);

            if (FindTag(ref stackCtx.hei))
            {
                stackCtx.SetRecordSourceToHashEntry(hlog);

                if (UseReadCache)
                    SkipReadCache(ref stackCtx);

                if (stackCtx.recSrc.LogicalAddress >= fromAddress)
                {
                    var physicalAddress = stackCtx.recSrc.SetPhysicalAddress();
                    ref RecordInfo recordInfo = ref hlog.GetInfo(physicalAddress);
                    if (recordInfo.Invalid || !comparer.Equals(ref key, ref hlog.GetKey(physicalAddress)))
                    {
                        logicalAddress = recordInfo.PreviousAddress;
                        TraceBackForKeyMatch(ref key, logicalAddress, fromAddress, out logicalAddress, out _);
                    }

                    if (stackCtx.recSrc.LogicalAddress < fromAddress)
                    {
                        logicalAddress = 0;
                        return new(StatusCode.NotFound);
                    }
                    logicalAddress = stackCtx.recSrc.LogicalAddress;
                    return new(StatusCode.Found);
                }
                logicalAddress = 0;
                return new(StatusCode.NotFound);
            }

            // no tag found
            logicalAddress = 0;
            return new(StatusCode.NotFound);
        }
    }
}
