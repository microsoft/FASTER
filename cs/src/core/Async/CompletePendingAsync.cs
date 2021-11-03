// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// The FASTER key-value store
    /// </summary>
    /// <typeparam name="Key">Key</typeparam>
    /// <typeparam name="Value">Value</typeparam>
    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        /// <summary>
        /// Check if at least one (sync) request is ready for CompletePending to operate on
        /// </summary>
        /// <param name="currentCtx"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        internal async ValueTask ReadyToCompletePendingAsync<Input, Output, Context>(FasterExecutionContext<Input, Output, Context> currentCtx, CancellationToken token = default)
        {
            #region Previous pending requests
            if (!RelaxedCPR)
            {
                if (currentCtx.phase == Phase.IN_PROGRESS)
                {
                    if (currentCtx.prevCtx.SyncIoPendingCount != 0)
                        await currentCtx.prevCtx.readyResponses.WaitForEntryAsync(token).ConfigureAwait(false);
                }
            }
            #endregion

            if (currentCtx.SyncIoPendingCount != 0)
                await currentCtx.readyResponses.WaitForEntryAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Complete outstanding pending operations that were issued synchronously
        /// Async operations (e.g., ReadAsync) need to be completed individually
        /// </summary>
        /// <returns></returns>
        internal async ValueTask CompletePendingAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                                      FasterExecutionContext<Input, Output, Context> currentCtx, CancellationToken token,
                                      CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs)
        {
            while (true)
            {
                bool done = true;

                #region Previous pending requests
                if (!RelaxedCPR)
                {
                    if (currentCtx.phase == Phase.IN_PROGRESS)
                    {
                        fasterSession.UnsafeResumeThread();
                        try
                        {
                            InternalCompletePendingRequests(currentCtx.prevCtx, currentCtx, fasterSession, completedOutputs);
                            InternalCompleteRetryRequests(currentCtx.prevCtx, currentCtx, fasterSession);
                        }
                        finally
                        {
                            fasterSession.UnsafeSuspendThread();
                        }
                        await currentCtx.prevCtx.WaitPendingAsync(token).ConfigureAwait(false);
                        done &= currentCtx.prevCtx.HasNoPendingRequests;
                    }
                }
                #endregion

                fasterSession.UnsafeResumeThread();
                try
                {
                    InternalCompletePendingRequests(currentCtx, currentCtx, fasterSession, completedOutputs);
                    InternalCompleteRetryRequests(currentCtx, currentCtx, fasterSession);
                }
                finally
                {
                    fasterSession.UnsafeSuspendThread();
                }

                await currentCtx.WaitPendingAsync(token).ConfigureAwait(false);
                done &= currentCtx.HasNoPendingRequests;

                if (done) return;

                InternalRefresh(currentCtx, fasterSession);

                Thread.Yield();
            }
        }
    }
}
