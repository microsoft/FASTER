using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using FASTER.libdpr;

namespace dpredis
{
    public class RedisStateObject : ISimpleStateObject<long>, IDisposable
    {
        private struct ReusableBuffer : IDisposableBuffer
        {
            // TODO(Tianyu): This will not handle situation where buffer is not returned on the same thread very well
            private ThreadLocalObjectPool<byte[]> pool;
            private byte[] underlying;

            public ReusableBuffer(ThreadLocalObjectPool<byte[]> pool, byte[] underlying)
            {
                this.pool = pool;
                this.underlying = underlying;
            }

            public void Dispose()
            {
                pool.Return(underlying);    
            }

            public Span<byte> Bytes()
            {
                return new Span<byte>(underlying);
            }
        }
        
        private ThreadLocalObjectPool<byte[]> reusableBuffers;
        private IPEndPoint redisBackend;
        private long lastCheckpointTime = 0;
        private string auth;
        
        private ThreadLocal<Socket> conn;

        public RedisStateObject(string name, int port, string auth)
        {
            reusableBuffers = new ThreadLocalObjectPool<byte[]>(() => new byte[4096]);
            redisBackend = new IPEndPoint(Dns.GetHostAddresses(name)[0], port);
            this.auth = auth;
            conn = new ThreadLocal<Socket>(() =>
            {
                var socket = new Socket(redisBackend.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                socket.Connect(redisBackend);
                socket.Send(System.Text.Encoding.ASCII.GetBytes($"*2\r\n$4\r\nAUTH\r\n${auth.Length}\r\n{auth}\r\n"));
                var buffer = reusableBuffers.Checkout();
                var len = socket.Receive(buffer);
                Debug.Assert(System.Text.Encoding.ASCII.GetString(buffer, 0, len).Equals("+OK"));
                reusableBuffers.Return(buffer);
                return socket;
            }, true);
        }
        
        public bool ProcessBatch(ReadOnlySpan<byte> request, out IDisposableBuffer reply)
        {
            var sock = conn.Value;
            var buffer = reusableBuffers.Checkout();
            sock.Send(request);
            sock.Receive(buffer);
            reply = new ReusableBuffer(reusableBuffers, buffer);
            return true;
        }

        public void PerformCheckpoint(Action<long> onPersist)
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

        public void RestoreCheckpoint(long token)
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