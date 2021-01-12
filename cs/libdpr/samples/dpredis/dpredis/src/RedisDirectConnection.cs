using System;
using System.Net.Sockets;
using System.Threading;

namespace dpredis
{

    public class RedisDirectConnection
    {
        internal class ConnState : MessageUtil.AbstractRedisConnState
        {
            private RedisDirectConnection conn;

            internal ConnState(RedisDirectConnection conn)
            {
                this.conn = conn;
            }
            
            protected override bool HandleRespMessage(byte[] buf, int start, int end)
            {
                Interlocked.Decrement(ref conn.outstandingCount);
                // Always ignore responses
                return true;
            }
        }
        
        private Socket socket;
        private byte[] messageBuffer;
        private int head = 0, commandCount = 0, outstandingCount = 0;

        private int batchSize, windowSize;
        
        public RedisDirectConnection(RedisShard shard, int batchSize = 1024, int windowSize = -1)
        {
            socket = MessageUtil.GetNewRedisConnection(shard);
            socket.NoDelay = true;
            var redisSaea = new SocketAsyncEventArgs();
            redisSaea.SetBuffer(new byte[1 << 20], 0, 1 << 20);
            redisSaea.UserToken = new ConnState(this);
            redisSaea.Completed += MessageUtil.AbstractRedisConnState.RecvEventArg_Completed;
            socket.ReceiveAsync(redisSaea);

            messageBuffer = new byte[1 << 20];
            this.batchSize = batchSize;
            this.windowSize = windowSize;
        }

        public void Flush()
        {
            socket.Send(new Span<byte>(messageBuffer, 0, head));
            head = 0;
            commandCount = 0;
        }

        public void SendSetCommand(long key, long value)
        {
            // Hold off while window is full
            while (outstandingCount == windowSize)
                Thread.Yield();
            
            while (true)
            {
                var newHead = head;
                newHead += MessageUtil.WriteRedisArrayHeader(3, messageBuffer, head);
                newHead += MessageUtil.WriteRedisBulkString("SET", messageBuffer, head);
                newHead += MessageUtil.WriteRedisBulkString(key, messageBuffer, head);
                newHead += MessageUtil.WriteRedisBulkString(value, messageBuffer, head);
                if (newHead == head)
                {
                    Flush();
                    continue;
                }

                head = newHead;
                commandCount++;
                Interlocked.Increment(ref outstandingCount);
                break;
            }
            
            if (commandCount == batchSize)
                Flush();
        }

        public void SendGetCommand(string key)
        {
            // Hold off while window is full
            while (outstandingCount == windowSize)
                Thread.Yield();
            
            while (true)
            {
                var newHead = head;
                newHead += MessageUtil.WriteRedisArrayHeader(2, messageBuffer, head);
                newHead += MessageUtil.WriteRedisBulkString("GET", messageBuffer, head);
                newHead += MessageUtil.WriteRedisBulkString(key, messageBuffer, head);
                if (newHead == head)
                {
                    Flush();
                    continue;
                }

                head = newHead;
                commandCount++;
                Interlocked.Increment(ref outstandingCount);
                break;
            }
            
            if (commandCount == batchSize)
                Flush();
        }

        public void WaitAll()
        {
            while (outstandingCount != 0)
                Thread.Yield();
        }
    }
}