// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#define CALLOC

using System;
using System.Threading;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace FASTER.core
{
    public static class Config
    {
        public static string CheckpointDirectory = "C:\\data";
    }

    public struct AsyncGetFromDiskResult<TContext> : IAsyncResult
    {
        public TContext context;

        public bool IsCompleted => throw new NotImplementedException();

        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        public object AsyncState => throw new NotImplementedException();

        public bool CompletedSynchronously => throw new NotImplementedException();
    }

    public struct PageAsyncReadResult<TContext> : IAsyncResult
    {
        public long page;
        public TContext context;
        public CountdownEvent handle;
        public SectorAlignedMemory freeBuffer1;
        public IOCompletionCallback callback;
        public int count;
        public IDevice objlogDevice;

        public bool IsCompleted => throw new NotImplementedException();

        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        public object AsyncState => throw new NotImplementedException();

        public bool CompletedSynchronously => throw new NotImplementedException();

        public void Free()
        {
            if (freeBuffer1.buffer != null)
                freeBuffer1.Return();

            if (handle != null)
            {
                handle.Signal();
            }
        }
    }

    public class PageAsyncFlushResult<TContext> : IAsyncResult
    {
        public long page;
        public TContext context;
        public bool partial;
        public long untilAddress;
        public int count;
        public CountdownEvent handle;
        public IDevice objlogDevice;
        public SectorAlignedMemory freeBuffer1;
        public SectorAlignedMemory freeBuffer2;


        public bool IsCompleted => throw new NotImplementedException();

        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        public object AsyncState => throw new NotImplementedException();

        public bool CompletedSynchronously => throw new NotImplementedException();

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

    public unsafe class HashIndexPageAsyncFlushResult : IAsyncResult
    {
        public HashBucket* start;
        public int numChunks;
        public int numIssued;
        public int numFinished;
        public uint chunkSize;
        public IDevice device;

        public bool IsCompleted => throw new NotImplementedException();

		public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

		public object AsyncState => throw new NotImplementedException();

		public bool CompletedSynchronously => throw new NotImplementedException();
	}

    public struct HashIndexPageAsyncReadResult : IAsyncResult
    {
        public int chunkIndex;

        public bool IsCompleted => throw new NotImplementedException();

        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        public object AsyncState => throw new NotImplementedException();

        public bool CompletedSynchronously => throw new NotImplementedException();
    }

    public struct OverflowPagesFlushAsyncResult : IAsyncResult
    {
        public bool IsCompleted => throw new NotImplementedException();

        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        public object AsyncState => throw new NotImplementedException();

        public bool CompletedSynchronously => throw new NotImplementedException();
    }

    public struct OverflowPagesReadAsyncResult : IAsyncResult
    {

        public bool IsCompleted => throw new NotImplementedException();

        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        public object AsyncState => throw new NotImplementedException();

        public bool CompletedSynchronously => throw new NotImplementedException();
    }

    public struct CountdownEventAsyncResult : IAsyncResult
    {
        public CountdownEvent countdown;
        public Action action;

        public bool IsCompleted => throw new NotImplementedException();

        public WaitHandle AsyncWaitHandle => throw new NotImplementedException();

        public object AsyncState => throw new NotImplementedException();

        public bool CompletedSynchronously => throw new NotImplementedException();
    }
}
