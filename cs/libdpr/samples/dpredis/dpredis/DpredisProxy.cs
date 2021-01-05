using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Transactions;
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

    internal class RedisMiddleMan
    {
        private Socket redisSocket, clientSocket;
        private SemaphoreSlim queueLatch;
        private Queue<DpredisBatchHandle> outstandingBatches;
        private SimpleObjectPool<DpredisBatchHandle> batchHandlePool;
        private DprManager<RedisStateObject, long> dprManager;

        public RedisMiddleMan(Socket clientSocket, SimpleObjectPool<DpredisBatchHandle> batchHandlePool,
            DprManager<RedisStateObject, long> dprManager)
        {
            redisSocket = null;
            this.clientSocket = clientSocket;
            queueLatch = new SemaphoreSlim(1, 1);
            outstandingBatches = new Queue<DpredisBatchHandle>();
            this.batchHandlePool = batchHandlePool;
            this.dprManager = dprManager;
        }

        internal unsafe void HandleNewRequest(byte[] buf, int offset, int size)
        {
            fixed (byte* b = buf)
            {
                ref var requestHeader = ref Unsafe.AsRef<DprBatchRequestHeader>(b + offset);
                var batchHandle = batchHandlePool.Checkout();
                var allowed = dprManager.RequestBatchBegin(
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
                dprManager.StateObject().OperationLatch().EnterReadLock();
                redisSocket ??= dprManager.StateObject().GetNewRedisConnection();
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
            currentBatch.tracker.MarkOperationRangesVersion(0, currentBatch.batchSize,
                dprManager.StateObject().Version());
            dprManager.StateObject().OperationLatch().ExitReadLock();
            var ret = dprManager.SignalBatchFinish(
                new Span<byte>(currentBatch.requestHeader),
                new Span<byte>(currentBatch.responseHeader), currentBatch.tracker);
            Debug.Assert(ret == 0);
            clientSocket.SendDpredisResponse(new Span<byte>(currentBatch.responseHeader),
                new Span<byte>(buffer, 0, messageEnd));
            return true;
        }
    }

    internal class DpredisServerInDprConnState : MessageUtil.AbstractDprConnState
    {
        private RedisMiddleMan middleMan;

        public DpredisServerInDprConnState(RedisMiddleMan middleMan)
        {
            this.middleMan = middleMan;
        }

        protected override void HandleMessage(byte[] buf, int offset, int size)
        {
            middleMan.HandleNewRequest(buf, offset, size);
        }
    }

    internal class DpredisServerOutConnState : MessageUtil.AbstractRedisConnState
    {
        private RedisMiddleMan middleMan;

        public DpredisServerOutConnState(RedisMiddleMan middleMan)
        {
            this.middleMan = middleMan;
        }
        protected override bool HandleSimpleString(byte[] buf, int start, int end)
        {
            return middleMan.HandleNewResponse(buf, end);
        }
    }

    public class DpredisProxy : IDisposable
    {
        private const int batchMaxSize = 1 << 20;
        private Socket servSocket;
        private string ip;
        private int port;
        private RedisShard backend;
        private DprManager<RedisStateObject, long> dprManager;
        private SimpleObjectPool<DpredisBatchHandle> batchHandlePool;

        public void StartServer()
        {
            var ipAddr = IPAddress.Parse(ip);
            var endPoint = new IPEndPoint(ipAddr, port);
            servSocket = new Socket(ipAddr.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            servSocket.Bind(endPoint);
            servSocket.Listen(512);

            var acceptEventArg = new SocketAsyncEventArgs();
        }

        private bool HandleNewConnection(SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                e.Dispose();
                return false;
            }

            var receiveEventArgs = new SocketAsyncEventArgs();
            receiveEventArgs.SetBuffer(new byte[batchMaxSize], 0, batchMaxSize);
            // receiveEventArgs.UserToken =
            //     new ServerConnectionState<Key, Value, Input, Output, Functions>(e.AcceptSocket, worker, threadPool);
            // receiveEventArgs.Completed += ServerConnectionState<Key, Value, Input, Output, Functions>.RecvEventArg_Completed;
            //
            // e.AcceptSocket.NoDelay = true;
            // // If the client already have packets, avoid handling it here on the handler so we don't block future accepts.
            // if (!e.AcceptSocket.ReceiveAsync(receiveEventArgs))
            //     Task.Run(() => ServerConnectionState<Key, Value, Input, Output, Functions>.RecvEventArg_Completed(null, receiveEventArgs));
            return true;
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}