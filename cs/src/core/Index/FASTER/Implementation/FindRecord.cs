// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using static FASTER.core.Utility;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordInMemory(ref Key key, ref OperationStackContext<Key, Value> stackCtx, long minAddress, bool stopAtHeadAddress = true)
        {
            if (UseReadCache && FindInReadCache(ref key, ref stackCtx, minAddress: Constants.kInvalidAddress))
                return true;
            if (minAddress < hlog.HeadAddress && stopAtHeadAddress)
                minAddress = hlog.HeadAddress;
            return TryFindRecordInMainLog(ref key, ref stackCtx, minAddress: minAddress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordInMemory<Input, Output, Context>(ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                                                                   ref PendingContext<Input, Output, Context> pendingContext)
        {
            // Add 1 to the pendingContext minAddresses because we don't want an inclusive search; we're looking to see if it was added *after*.
            if (UseReadCache)
            { 
                var minRC = IsReadCache(pendingContext.InitialEntryAddress) ? pendingContext.InitialEntryAddress + 1 : Constants.kInvalidAddress;
                if (FindInReadCache(ref key, ref stackCtx, minAddress: minRC))
                    return true;
            }
            var minLog = pendingContext.InitialLatestLogicalAddress < hlog.HeadAddress ? hlog.HeadAddress : pendingContext.InitialLatestLogicalAddress + 1;
            return TryFindRecordInMainLog(ref key, ref stackCtx, minAddress: minLog);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryFindRecordInMainLog(ref Key key, ref OperationStackContext<Key, Value> stackCtx, long minAddress)
        {
            Debug.Assert(!stackCtx.recSrc.HasInMemorySrc, "Should not have found record before this call");
            if (stackCtx.recSrc.LogicalAddress >= minAddress)
            {
                stackCtx.recSrc.SetPhysicalAddress();
                TraceBackForKeyMatch(ref key, ref stackCtx.recSrc, minAddress);
            }
            return stackCtx.recSrc.HasInMemorySrc;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryFindRecordInMainLogMemory(ref Key key, ref OperationStackContext<Key, Value> stackCtx, long minAddress, out bool needIO)
        {
            // minAddress is inclusive
            if (!FindTag(ref stackCtx.hei))
                return needIO = false;
            stackCtx.SetRecordSourceToHashEntry(hlog);

            if (!stackCtx.hei.IsReadCache)
            {
                if (stackCtx.hei.Address < minAddress)
                    return needIO = false;
                if (stackCtx.hei.Address < hlog.HeadAddress)
                {
                    needIO = stackCtx.hei.Address >= hlog.BeginAddress;
                    return false;
                }
            }

            if (UseReadCache)
                SkipReadCache(ref stackCtx, out _); // Where this is called, we have no dependency on source addresses so we don't care if it Refreshed

            var result = TryFindRecordInMainLog(ref key, ref stackCtx, minAddress < hlog.HeadAddress ? hlog.HeadAddress : minAddress);
            needIO = stackCtx.recSrc.LogicalAddress < hlog.HeadAddress && stackCtx.recSrc.LogicalAddress >= hlog.BeginAddress;
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(ref Key key, ref RecordSource<Key, Value> recSrc, long minAddress)
        {
            // PhysicalAddress must already be populated by callers.
            ref var recordInfo = ref recSrc.GetInfo();
            if (!recordInfo.Invalid && comparer.Equals(ref key, ref recSrc.GetKey()))
                return recSrc.HasMainLogSrc = true;

            recSrc.LogicalAddress = recordInfo.PreviousAddress;
            return recSrc.HasMainLogSrc = TraceBackForKeyMatch(ref key, recSrc.LogicalAddress, minAddress, out recSrc.LogicalAddress, out recSrc.PhysicalAddress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(ref Key key, long fromLogicalAddress, long minAddress, out long foundLogicalAddress, out long foundPhysicalAddress)
        {
            // This overload is called when the record at the "current" logical address does not match 'key'; fromLogicalAddress is its .PreviousAddress.
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
