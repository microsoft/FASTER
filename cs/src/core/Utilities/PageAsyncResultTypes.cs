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
    public class PageAsyncReadResult<TContext> : IAsyncResult
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

        internal CountdownEvent handle;
        internal SectorAlignedMemory freeBuffer1;
        internal SectorAlignedMemory freeBuffer2;
        internal IOCompletionCallback callback;
        internal IDevice objlogDevice;
        internal long resumeptr;
        internal long untilptr;
        internal object frame;
        internal int frameSize;

        /// <summary>
        /// 
        /// </summary>
        public bool IsCompleted => throw new NotImplementedException();

        /// <summary>
        /// 
        /// </summary>
        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        /// <summary>
        /// 
        /// </summary>
        public object AsyncState => throw new NotImplementedException();

        /// <summary>
        /// 
        /// </summary>
        public bool CompletedSynchronously => throw new NotImplementedException();

        /// <summary>
        /// Free
        /// </summary>
        public void Free()
        {
            if (freeBuffer1.buffer != null)
            {
                freeBuffer1.Return();
                freeBuffer1.buffer = null;
            }

            if (freeBuffer2.buffer != null)
            {
                freeBuffer2.Return();
                freeBuffer2.buffer = null;
            }
        }
    }

    /// <summary>
    /// Page async flush result
    /// </summary>
    /// <typeparam name="TContext"></typeparam>
    public class PageAsyncFlushResult<TContext> : IAsyncResult
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
        internal CountdownEvent handle;
        internal IDevice objlogDevice;
        internal SectorAlignedMemory freeBuffer1;
        internal SectorAlignedMemory freeBuffer2;
        internal AutoResetEvent done;

        /// <summary>
        /// 
        /// </summary>
        public bool IsCompleted => throw new NotImplementedException();

        /// <summary>
        /// 
        /// </summary>
        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        /// <summary>
        /// 
        /// </summary>
        public object AsyncState => throw new NotImplementedException();

        /// <summary>
        /// 
        /// </summary>
        public bool CompletedSynchronously => throw new NotImplementedException();

        /// <summary>
        /// Free
        /// </summary>
        public void Free()
        {
            if (freeBuffer1.buffer != null)
                freeBuffer1.Return();
            if (freeBuffer2.buffer != null)
                freeBuffer2.Return();

            if (handle != null)
            {
                handle.Signal();
            }
        }
    }
}
