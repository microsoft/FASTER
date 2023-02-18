// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SynchronizeEpoch<Input, Output, Context, FasterSession>(
            FasterExecutionContext<Input, Output, Context> opCtx,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            ref PendingContext<Input, Output, Context> pendingContext,
            FasterSession fasterSession)
            where FasterSession : IFasterSession
        {
            var version = opCtx.version;
            Debug.Assert(currentCtx.version == version);
            Debug.Assert(currentCtx.phase == Phase.PREPARE);
            InternalRefresh(currentCtx, fasterSession);
            Debug.Assert(currentCtx.version > version);

            pendingContext.version = currentCtx.version;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void HeavyEnter<Input, Output, Context, FasterSession>(long hash, FasterExecutionContext<Input, Output, Context> ctx, FasterSession session)
            where FasterSession : IFasterSession
        {
            if (ctx.phase == Phase.PREPARE_GROW)
            {
                // We spin-wait as a simplification
                // Could instead do a "heavy operation" here
                while (systemState.Phase != Phase.IN_PROGRESS_GROW)
                    Thread.SpinWait(100);
                InternalRefresh(ctx, session);
            }
            if (ctx.phase == Phase.IN_PROGRESS_GROW)
            {
                SplitBuckets(hash);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SpinWaitUntilClosed(long address)
        {
            // Unlike HeadAddress, ClosedUntilAddress is a high-water mark; a record that is == to ClosedUntilAddress has *not* been closed yet.
            while (address >= this.hlog.ClosedUntilAddress)
            {
                Debug.Assert(address < hlog.HeadAddress);
                epoch.ProtectAndDrain();
                Thread.Yield();
            }
        }
    }
}
