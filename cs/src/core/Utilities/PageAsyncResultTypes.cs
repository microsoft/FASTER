// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#define CALLOC

using System;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Result of async page read
    /// </summary>
    /// <typeparam name="TContext"></typeparam>
    public sealed class PageAsyncReadResult<TContext>
    {
        internal long page;
        internal long offset;
        internal TContext context;
        internal CountdownEvent handle;
        internal SectorAlignedMemory freeBuffer1;
        internal SectorAlignedMemory freeBuffer2;
        internal DeviceIOCompletionCallback callback;
        internal IDevice objlogDevice;
        internal object frame;
        internal CancellationTokenSource cts;

        /* Used for iteration */
        internal long resumePtr;
        internal long untilPtr;
        internal long maxPtr;

        /// <summary>
        /// Free
        /// </summary>
        public void Free()
        {
            if (freeBuffer1 != null)
            {
                freeBuffer1.Return();
                freeBuffer1 = null;
            }

            if (freeBuffer2 != null)
            {
                freeBuffer2.Return();
                freeBuffer2 = null;
            }
        }
    }

    /// <summary>
    /// Shared flush completion tracker, when bulk-flushing many pages
    /// </summary>
    public class FlushCompletionTracker
    {
        /// <summary>
        /// Semaphore to set on flush completion
        /// </summary>
        public SemaphoreSlim completedSemaphore;

        /// <summary>
        /// Number of pages being flushed
        /// </summary>
        public int count;
    }

    /// <summary>
    /// Page async flush result
    /// </summary>
    /// <typeparam name="TContext"></typeparam>
    public sealed class PageAsyncFlushResult<TContext>
    {
        /// <summary>
        /// Page
        /// </summary>
        public long page;
        /// <summary>
        /// Context
        /// </summary>
        public TContext context;
        /// <summary>
        /// Count
        /// </summary>
        public int count;

        internal bool partial;
        internal long fromAddress;
        internal long untilAddress;
        internal SectorAlignedMemory freeBuffer1;
        internal SectorAlignedMemory freeBuffer2;
        internal AutoResetEvent done;
        internal SemaphoreSlim completedSemaphore;
        internal FlushCompletionTracker flushCompletionTracker;

        /// <summary>
        /// Free
        /// </summary>
        public void Free()
        {
            if (freeBuffer1 != null)
            {
                freeBuffer1.Return();
                freeBuffer1 = null;
            }
            if (freeBuffer2 != null)
            {
                freeBuffer2.Return();
                freeBuffer2 = null;
            }

            completedSemaphore?.Release();
            flushCompletionTracker?.completedSemaphore?.Release();
        }
    }
}
