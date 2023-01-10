// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using static FASTER.core.LockUtility;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordInMemory(ref Key key, ref OperationStackContext<Key, Value> stackCtx, long minOffset, bool waitForTentative = true)
        {
            if (!UseReadCache || !FindInReadCache(ref key, ref stackCtx, untilAddress: Constants.kInvalidAddress))
            {
                TryFindRecordInMainLog(ref key, ref stackCtx, minOffset, waitForTentative);
            }
            return stackCtx.recSrc.HasInMemorySrc;
        }

        private bool TryFindRecordInMainLog(ref Key key, ref OperationStackContext<Key, Value> stackCtx, long minOffset, bool waitForTentative)
        {
            Debug.Assert(!stackCtx.recSrc.HasInMemorySrc, "Should not have found record before this call");
            if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
            {
                stackCtx.recSrc.PhysicalAddress = hlog.GetPhysicalAddress(stackCtx.recSrc.LogicalAddress);
                TraceBackForKeyMatch(ref key, ref stackCtx.recSrc, minOffset, waitForTentative);
            }
            return stackCtx.recSrc.HasInMemorySrc;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(ref Key key, ref RecordSource<Key, Value> recSrc, long minOffset, bool waitForTentative = true)
        {
            ref var recordInfo = ref hlog.GetInfo(recSrc.PhysicalAddress);
            if (comparer.Equals(ref key, ref hlog.GetKey(recSrc.PhysicalAddress)) && !recordInfo.Invalid)
            {
                if (!waitForTentative || SpinWaitWhileTentativeAndReturnValidity(ref recordInfo))
                    return recSrc.HasMainLogSrc = true;
            }
            recSrc.LogicalAddress = recordInfo.PreviousAddress;
            return recSrc.HasMainLogSrc = TraceBackForKeyMatch(ref key, recSrc.LogicalAddress, minOffset, out recSrc.LogicalAddress, out recSrc.PhysicalAddress, waitForTentative);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(ref Key key, long fromLogicalAddress, long minAddress, out long foundLogicalAddress, out long foundPhysicalAddress, bool waitForTentative = true)
        {
            foundLogicalAddress = fromLogicalAddress;
            while (foundLogicalAddress >= minAddress)
            {
                foundPhysicalAddress = hlog.GetPhysicalAddress(foundLogicalAddress);

                ref var recordInfo = ref hlog.GetInfo(foundPhysicalAddress);
                if (!recordInfo.Invalid && comparer.Equals(ref key, ref hlog.GetKey(foundPhysicalAddress)))
                {
                    if (!waitForTentative || SpinWaitWhileTentativeAndReturnValidity(ref recordInfo))
                        return true;
                }

                foundLogicalAddress = recordInfo.PreviousAddress;
            }
            foundPhysicalAddress = Constants.kInvalidAddress;
            return false;
        }
    }
}
