using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Net.Sockets;
using System.Threading;
using FASTER.core;

namespace dpredis
{
    // One per client conncetion --- never concurrent sends or concurrent receives, but they may be concurrent w.r.t.
    // each other
    internal class RedisForwardConnection
    {
        private Socket redisSocket;
        private SemaphoreSlim latch;
        private Queue<DpredisBatchHandle> outstandingBatches;

        private int readHead, bytesRead;
        private bool inString = false;

        public RedisConnection(RedisStateObject shard)
        {
            redisSocket = shard.GetNewRedisConnection();
            outstandingBatches = new Queue<DpredisBatchHandle>();
        }

        internal void SendBatch(DpredisBatchHandle handle, Span<byte> command)
        {
            latch.Wait();
            outstandingBatches.Enqueue(handle);
            latch.Release();
            redisSocket.Send(command);
        }

        internal void MarkMessageEnd(byte[] buffer)
        {
            var handle = outstandingBatches.Peek();
            handle.messagesLeft--;
            // This whole batch is done. Can start sending reply to client
            if (handle.messagesLeft == 0)
            {
                
            }
        }
        
        private static void HandleReceiveCompletion(SocketAsyncEventArgs e)
        {
            var connState = (RedisConnection) e.UserToken;
            if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success)
            {
                connState.redisSocket.Dispose();
                e.Dispose();
                return;
            }

            connState.bytesRead += e.BytesTransferred;
            // TODO(Tianyu): Only supports simple interface for now
            for (; connState.readHead >= connState.bytesRead; connState.readHead++)
            {
                // Beginning a simple string with +
                if (e.Buffer[connState.readHead] == '+')
                {
                    Debug.Assert(!connState.inString);
                    connState.inString = true;
                } 
                // Ending a simple string with \r\n
                else if (e.Buffer[connState.readHead] == '\n'
                         // Never yields out-of-bound because no well-formed buffer starts with \n
                         && e.Buffer[connState.readHead - 1] == '\r')
                {
                    Debug.Assert(connState.inString);
                    connState.inString = false;
                    // This is one completed reply from the queue. Mark it off.
                }
            }
            while (true)
            {
                // Continuously scan for simple string header
                for (; e.Buffer[connState.readHead] != '+'; connState.readHead++)
                {
                    // No more characters 
                    if (connState.readHead >= connState.bytesRead) return;
                }
                connState.readHead++;
            }
            var newHead = connState.TryConsumeMessages(e.Buffer);
            e.SetBuffer(newHead, e.Buffer.Length - newHead);
            return true;
        }

        public static void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            var connState = (RedisConnection) e.UserToken;
            do
            {
                // No more things to receive
                HandleReceiveCompletion(e);
            } while (!connState.redisSocket.ReceiveAsync(e));
        }    
    }
}