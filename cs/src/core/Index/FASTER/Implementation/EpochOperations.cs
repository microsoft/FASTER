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
            FasterExecutionContext<Input, Output, Context> sessionCtx,
            ref PendingContext<Input, Output, Context> pendingContext,
            FasterSession fasterSession)
            where FasterSession : IFasterSession
        {
            var version = sessionCtx.version;
            Debug.Assert(sessionCtx.version == version);
            Debug.Assert(sessionCtx.phase == Phase.PREPARE);
            InternalRefresh(sessionCtx, fasterSession);
            Debug.Assert(sessionCtx.version > version);

            pendingContext.version = sessionCtx.version;
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
