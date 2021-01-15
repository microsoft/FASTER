using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using FASTER.libdpr;

namespace dpredis
{
 internal class DpredisBatchHandle
    {
        // TODO(Tianyu): Magic number
        public byte[] requestHeader = new byte[4096];
        public byte[] responseHeader = new byte[4096];
        public DprBatchVersionTracker tracker;
        public int batchSize, messagesLeft;
    }

    internal class RedisMiddleMan : IDisposable
    {
        internal Socket clientSocket, redisSocket;
        private SemaphoreSlim queueLatch;
        private Queue<DpredisBatchHandle> outstandingBatches;
        private SimpleObjectPool<DpredisBatchHandle> batchHandlePool;
        private DprServer<RedisStateObject, long> dprServer;

        public RedisMiddleMan(Socket clientSocket, Socket redisSocket, SimpleObjectPool<DpredisBatchHandle> batchHandlePool,
            DprServer<RedisStateObject, long> dprServer)
        {
            this.clientSocket = clientSocket;
            this.redisSocket = redisSocket;
            queueLatch = new SemaphoreSlim(1, 1);
            outstandingBatches = new Queue<DpredisBatchHandle>();
            this.batchHandlePool = batchHandlePool;
            this.dprServer = dprServer;
        }

        public void Dispose()
        {
            queueLatch.Wait();
            if (outstandingBatches.Count != 0) throw new Exception();
            queueLatch.Release();
            redisSocket.Dispose();
            clientSocket.Dispose();
        }

        internal unsafe void HandleNewRequest(byte[] buf, int offset, int size)
        {
            fixed (byte* b = buf)
            {
                ref var requestHeader = ref Unsafe.AsRef<DprBatchRequestHeader>(b + offset);
                var batchHandle = batchHandlePool.Checkout();
                var allowed = dprServer.RequestBatchBegin(
                    new Span<byte>(b + offset, requestHeader.Size()),
                    new Span<byte>(batchHandle.responseHeader),
                    out var tracker);

                fixed (byte* h = batchHandle.requestHeader)
                {
                    Buffer.MemoryCopy(b + offset, h, requestHeader.Size(), requestHeader.Size());
                }
                batchHandle.tracker = tracker;
                batchHandle.batchSize = requestHeader.numMessages;
                batchHandle.messagesLeft = requestHeader.numMessages;

                if (!allowed)
                {
                    // Will send back DPR-related error message
                    clientSocket.SendDpredisResponse(new Span<byte>(batchHandle.responseHeader), Span<byte>.Empty);
                    batchHandlePool.Return(batchHandle);
                    return;
                }

                // Otherwise, add to tracking and send to redis
                queueLatch.Wait();
                outstandingBatches.Enqueue(batchHandle);
                queueLatch.Release();
                var version = dprServer.StateObject().VersionScheme().Enter();
                batchHandle.tracker.MarkOperationRangesVersion(0, batchHandle.batchSize, version);
                redisSocket.Send(new Span<byte>(buf, offset + requestHeader.Size(), size - requestHeader.Size()));
            }
        }

        internal bool HandleNewResponse(byte[] buffer, int messageEnd)
        {
            queueLatch.Wait();
            var currentBatch = outstandingBatches.Peek();
            currentBatch.messagesLeft--;
            // Still more responses left in the batch
            if (currentBatch.messagesLeft != 0) return false;
            // Otherwise, an entire batch has been acked. Signal batch finish to dpr and forward reply to client
            outstandingBatches.Dequeue();

            dprServer.StateObject().VersionScheme().Leave();
            var ret = dprServer.SignalBatchFinish(
                new Span<byte>(currentBatch.requestHeader),
                new Span<byte>(currentBatch.responseHeader), currentBatch.tracker);
            Debug.Assert(ret == 0);
            clientSocket.SendDpredisResponse(new Span<byte>(currentBatch.responseHeader),
                new Span<byte>(buffer, 0, messageEnd));
            return true;
        }
    }

}