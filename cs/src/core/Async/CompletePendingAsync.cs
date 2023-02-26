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
        /// <param name="sessionCtx"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        internal static ValueTask ReadyToCompletePendingAsync<Input, Output, Context>(FasterExecutionContext<Input, Output, Context> sessionCtx, CancellationToken token = default) 
            => sessionCtx.WaitPendingAsync(token);

        /// <summary>
        /// Complete outstanding pending operations that were issued synchronously
        /// Async operations (e.g., ReadAsync) need to be completed individually
        /// </summary>
        /// <returns></returns>
        internal async ValueTask CompletePendingAsync<Input, Output, Context, FasterSession>(FasterSession fasterSession,
                                      CancellationToken token, CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            while (true)
            {
                fasterSession.UnsafeResumeThread();
                try
                {
                    InternalCompletePendingRequests(fasterSession, completedOutputs);
                }
                finally
                {
                    fasterSession.UnsafeSuspendThread();
                }

                await fasterSession.Ctx.WaitPendingAsync(token).ConfigureAwait(false);

                if (fasterSession.Ctx.HasNoPendingRequests) return;

                InternalRefresh<Input, Output, Context, FasterSession>(fasterSession);

                Thread.Yield();
            }
        }
    }
}
