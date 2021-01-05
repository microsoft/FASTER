using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using FASTER.libdpr;

namespace dpredis
{
    public struct RedisShard
    {
        public string name;
        public int port;
        public long id;
        public string auth;
    }

    public class RedisStateObject : SimpleStateObject<long>, IDisposable
    {
        private RedisShard shard;
        private ThreadLocalObjectPool<byte[]> reusableBuffers;
        private IPEndPoint redisBackend;
        private long lastCheckpointTime = 0;
        private string auth;
        
        private ThreadLocal<Socket> conn;
        
        private DprWorkerCallbacks<long> callbacks;
        private long versionCounter;
        private ReaderWriterLockSlim opLatch;

        public RedisStateObject(RedisShard shard)
        {
            this.shard = shard;
            reusableBuffers = new ThreadLocalObjectPool<byte[]>(() => new byte[4096]);
            redisBackend = new IPEndPoint(Dns.GetHostAddresses(shard.name)[0], shard.port);
            this.auth = auth;
            conn = new ThreadLocal<Socket>(GetNewRedisConnection, true);
        }

        public Socket GetNewRedisConnection()
        {
            var socket = new Socket(redisBackend.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(redisBackend);
            socket.Send(System.Text.Encoding.ASCII.GetBytes($"*2\r\n$4\r\nAUTH\r\n${auth.Length}\r\n{auth}\r\n"));
            var buffer = reusableBuffers.Checkout();
            var len = socket.Receive(buffer);
            Debug.Assert(System.Text.Encoding.ASCII.GetString(buffer, 0, len).Equals("+OK"));
            reusableBuffers.Return(buffer);
            return socket;
        }
        
        protected override void PerformCheckpoint(Action<long> onPersist)
        {
            var sock = conn.Value;
            var buffer = reusableBuffers.Checkout();
            while (true)
            {
                // Send checkpoint request
                sock.Send(System.Text.Encoding.ASCII.GetBytes("*1\r\n$6\r\nBGSAVE\r\n"));
                var len = sock.Receive(buffer);
                // Error means another checkpoint is in progress. Retry until that checkpoint completes
                if ((char) buffer[0] == '-') continue;
                Debug.Assert(System.Text.Encoding.ASCII.GetString(buffer, 0, len).Equals("+Background saving started"));
                break;
            }
            
            // TODO(Tianyu): Will this be invoked concurrently?
            Task.Run(async () =>
            {
                var previousSave = lastCheckpointTime;
                while (true)
                {
                    sock.Send(System.Text.Encoding.ASCII.GetBytes("*1\r\n$8\r\nLASTSAVE\r\n"));
                    var len = sock.Receive(buffer);

                        lastCheckpointTime = long.Parse(System.Text.Encoding.ASCII.GetString(buffer, 0, len));
                        if (previousSave != lastCheckpointTime)
                        {
                            onPersist(lastCheckpointTime);
                            return;
                        }
                        await Task.Delay(10);
                }
            });
        }

        protected override void RestoreCheckpoint(long token)
        {
            // TODO(Tianyu): Apparently will need to restart Redis for this...
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            foreach (var val in conn.Values)
            {
                val.Dispose();
            }
        }
    }
}