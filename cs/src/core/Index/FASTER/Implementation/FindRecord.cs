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
        private bool TraceBackForKeyMatch(ref Key key, ref RecordSource<Key, Value> recSrc, long minAddress)
        {
            // PhysicalAddress must already be populated by callers.
            ref var recordInfo = ref hlog.GetInfo(recSrc.PhysicalAddress);
            if (!recordInfo.Invalid && comparer.Equals(ref key, ref hlog.GetKey(recSrc.PhysicalAddress)))
                return recSrc.HasMainLogSrc = true;

            recSrc.LogicalAddress = recordInfo.PreviousAddress;
            return recSrc.HasMainLogSrc = TraceBackForKeyMatch(ref key, recSrc.LogicalAddress, minAddress, out recSrc.LogicalAddress, out recSrc.PhysicalAddress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsRecordInReadCacheAndMainLog(ref Key key, ref RecordSource<Key, Value> recSrc)
        {
            // For updates, after we have found a record in the readcache we must make sure it is not also in the main log, because when the readcache is enabled we
            // maintain two insertion points: the HashBucketEntry (readcache records) and the splice location (main log records). If we do not verify the key does not
            // have a valid record in the main log, we may have a timing gap that allows a variation of the lost-update anomaly. Assume we have the following 3 threads:
            //  - T1: is doing Upsert (could be RMW also; using Upsert for simplicity)
            //  - T2: is doing CTT
            //  - T3: is doing RMW
            // Then the following series of actions on key1 could insert a value based on a stale source:
            //  - T1 scans in-memory for key1 and does not find it
            //  - T2 Inserts rcRec1 for key1 to RC (T2 is now done for this scenario)
            //  - T1 splices logRec1 for key1 into the readcache/log gap (the lowest readcache record now points to logRec1)
            //  - Now T3 calls FindInReadCache and finds Valid rcRec1 as source
            //      - FindInReadCache has populated T3's recSrc's latestLogicalAddress with the address of logRec1
            //      - (Here is the problem) A record should not be in readcache and main log, so having found rcRec1, T3 does not search the main log below latestLogicalAddress;
            //        T3 therefore does not know logRec1 is for key1.
            //      - T3 XLocks rcRec1
            //  - T1 does its post-insert check of the readcache tail, finds rcRec1, and invalidates it (T1 is now done for this scenario)
            //  - T3 continues with it update, but now its RMW is based on an obsolete source record, so its insertion loses T1's update.
            // Therefore, despite the inefficiency, updates must scan the main log for a key even after finding it in the readcache.
            long minAddress = hlog.ReadOnlyAddress;
            if (!recSrc.HasReadCacheSrc || recSrc.LatestLogicalAddress < minAddress)
                return false;
            return TraceBackForKeyMatch(ref key, recSrc.LatestLogicalAddress, minAddress, out _, out _);
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
