using System;
using System.Collections.Concurrent;
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
        public byte[] requestHeader = new byte[16384];
        public byte[] responseHeader = new byte[16384];
        public DprBatchVersionTracker tracker;
        public int batchSize, messagesLeft;
    }

    internal class RedisMiddleMan : IDisposable
    {
        internal Socket clientSocket, redisSocket;
        private ConcurrentQueue<DpredisBatchHandle> outstandingBatches;
        private SimpleObjectPool<DpredisBatchHandle> batchHandlePool;
        private DprServer<RedisStateObject, long> dprServer;

        public RedisMiddleMan(Socket clientSocket, Socket redisSocket, SimpleObjectPool<DpredisBatchHandle> batchHandlePool,
            DprServer<RedisStateObject, long> dprServer)
        {
            this.clientSocket = clientSocket;
            this.redisSocket = redisSocket;
            outstandingBatches = new ConcurrentQueue<DpredisBatchHandle>();
            this.batchHandlePool = batchHandlePool;
            this.dprServer = dprServer;
        }

        public void Dispose()
        {
            if (!outstandingBatches.IsEmpty) throw new Exception();
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
                outstandingBatches.Enqueue(batchHandle);
                var version = dprServer.StateObject().VersionScheme().Enter();
                batchHandle.tracker.MarkOperationRangesVersion(0, batchHandle.batchSize, version);
                redisSocket.Send(new Span<byte>(buf, offset + requestHeader.Size(), size - requestHeader.Size()));
            }
        }

        internal bool HandleNewResponse(byte[] buffer, int batchStart, int messageStart, int messageEnd)
        {
            var ret = outstandingBatches.TryPeek(out var currentBatch);
            Debug.Assert(ret);
            currentBatch.messagesLeft--;
            
            // Still more responses left in the batch
            if (currentBatch.messagesLeft != 0) return false;
            // Otherwise, an entire batch has been acked. Signal batch finish to dpr and forward reply to client
            outstandingBatches.TryDequeue(out _);
            dprServer.StateObject().VersionScheme().Leave();
            var s = dprServer.SignalBatchFinish(
                new Span<byte>(currentBatch.requestHeader),
                new Span<byte>(currentBatch.responseHeader), currentBatch.tracker);
            Debug.Assert(s == 0);
            clientSocket.SendDpredisResponse(new Span<byte>(currentBatch.responseHeader),
                new Span<byte>(buffer, batchStart, messageEnd - batchStart));
            batchHandlePool.Return(currentBatch);
            return true;
        }
    }

}