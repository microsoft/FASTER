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
        private bool TryFindRecordInMemory(ref Key key, ref OperationStackContext<Key, Value> stackCtx, long minOffset)
        {
            if (!UseReadCache || !FindInReadCache(ref key, ref stackCtx, untilAddress: Constants.kInvalidAddress))
            {
                TryFindRecordInMainLog(ref key, ref stackCtx, minOffset);
            }
            return stackCtx.recSrc.HasInMemorySrc;
        }

        internal bool TryFindRecordInMainLog(ref Key key, ref OperationStackContext<Key, Value> stackCtx, long minOffset)
        {
            Debug.Assert(!stackCtx.recSrc.HasInMemorySrc, "Should not have found record before this call");
            if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
            {
                stackCtx.recSrc.SetPhysicalAddress();
                TraceBackForKeyMatch(ref key, ref stackCtx.recSrc, minOffset);
            }
            return stackCtx.recSrc.HasInMemorySrc;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(ref Key key, ref RecordSource<Key, Value> recSrc, long minOffset)
        {
            ref var recordInfo = ref hlog.GetInfo(recSrc.PhysicalAddress);
            if (comparer.Equals(ref key, ref hlog.GetKey(recSrc.PhysicalAddress)) && !recordInfo.Invalid)
                return recSrc.HasMainLogSrc = true;

            recSrc.LogicalAddress = recordInfo.PreviousAddress;
            return recSrc.HasMainLogSrc = TraceBackForKeyMatch(ref key, recSrc.LogicalAddress, minOffset, out recSrc.LogicalAddress, out recSrc.PhysicalAddress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(ref Key key, long fromLogicalAddress, long minAddress, out long foundLogicalAddress, out long foundPhysicalAddress)
        {
            foundLogicalAddress = fromLogicalAddress;
            while (foundLogicalAddress >= minAddress)
            {
                foundPhysicalAddress = hlog.GetPhysicalAddress(foundLogicalAddress);

                ref var recordInfo = ref hlog.GetInfo(foundPhysicalAddress);
                if (!recordInfo.Invalid && comparer.Equals(ref key, ref hlog.GetKey(foundPhysicalAddress)))
                    return true;

                foundLogicalAddress = recordInfo.PreviousAddress;
            }
            foundPhysicalAddress = Constants.kInvalidAddress;
            return false;
        }
    }
}
