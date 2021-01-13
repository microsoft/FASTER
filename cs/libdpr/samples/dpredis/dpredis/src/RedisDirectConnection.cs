using System;
using System.Net.Sockets;
using System.Threading;

namespace dpredis
{

    
    public class RedisDirectConnection : IDisposable
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
        private RedisClientBuffer clientBuffer;
        private int outstandingCount = 0;

        private int batchSize, windowSize;

        public void Dispose()
        {
            Flush();
            WaitAll();
            socket.Dispose();
        }
        
        public RedisDirectConnection(RedisShard shard, int batchSize = 1024, int windowSize = -1)
        {
            socket = MessageUtil.GetNewRedisConnection(shard);
            socket.NoDelay = true;
            var redisSaea = new SocketAsyncEventArgs();
            redisSaea.SetBuffer(new byte[RedisClientBuffer.MAX_BUFFER_SIZE]);
            redisSaea.UserToken = new ConnState(this);
            redisSaea.Completed += MessageUtil.AbstractRedisConnState.RecvEventArg_Completed;
            socket.ReceiveAsync(redisSaea);

            clientBuffer = new RedisClientBuffer();
            this.batchSize = batchSize;
            this.windowSize = windowSize;
        }

        public void Flush()
        {
            socket.Send(clientBuffer.GetCurrentBytes());
            clientBuffer.Reset();
        }

        public void SendSetCommand(ulong key, ulong value)
        {
            // Hold off while window is full
            while (outstandingCount == windowSize)
                Thread.Yield();
            
            while (!clientBuffer.TryAddSetCommand(key, value)) Flush();
    
            if (clientBuffer.CommandCount() == batchSize)
                Flush();
        }

        public void SendGetCommand(ulong key)
        {
            // Hold off while window is full
            while (outstandingCount == windowSize)
                Thread.Yield();
            
            while (!clientBuffer.TryAddGetCommand(key)) Flush();
    
            if (clientBuffer.CommandCount() == batchSize)
                Flush();
        }

        public void WaitAll()
        {
            while (outstandingCount != 0)
                Thread.Yield();
        }
    }
}
