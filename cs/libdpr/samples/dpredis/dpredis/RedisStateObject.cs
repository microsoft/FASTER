using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using FASTER.libdpr;

namespace dpredis
{
    public class RedisStateObject : ISimpleStateObject<long>, IDisposable
    {
        private ThreadLocal<ThreadLocalObjectPool<byte[]>> reusableBuffers;
        private IPEndPoint redisBackend;
        private string auth;
        
        private ThreadLocal<Socket> conn;

        public RedisStateObject(string name, int port, string auth)
        {
            reusableBuffers = new ThreadLocal<ThreadLocalObjectPool<byte[]>>(() =>
                new ThreadLocalObjectPool<byte[]>(() => new byte[4096]));
            redisBackend = new IPEndPoint(Dns.GetHostAddresses(name)[0], port);
            this.auth = auth;
            conn = new ThreadLocal<Socket>(() =>
            {
                var socket = new Socket(redisBackend.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                socket.Connect(redisBackend);
                return socket;
            }, true);
        }
        
        public bool ProcessBatch(ReadOnlySpan<byte> request, out Span<byte> reply)
        {
            var sock = conn.Value;
            var buffer = reusableBuffers.Value.Checkout();
            sock.Send(request);
            sock.Receive(buffer);
            // TODO(Tianyu): Figure out what to do for memory management for reply
            reply = default;
            return true;
        }

        public long PerformCheckpoint(Action<long> onPersist)
        {
            throw new NotImplementedException();
        }

        public void RestoreCheckpoint(long token)
        {
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