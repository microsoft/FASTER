using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace dpredis
{
    public class ForwardSocketConn
    {
        private Socket src, dst;

        public ForwardSocketConn(Socket src, Socket dst)
        {
            this.src = src;
            this.dst = dst;
        }
        
        private static bool HandleReceiveCompletion(SocketAsyncEventArgs e)
        {
            var conn = (ForwardSocketConn) e.UserToken;
            if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success)
            {
                conn.src.Dispose();
                conn.dst.Dispose();
                e.Dispose();
                return false;
            }
            conn.dst.Send(new Span<byte>(e.Buffer, 0, e.BytesTransferred));
            e.SetBuffer(0, e.Buffer.Length);
            return true;
        }

        public static void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            var conn = (ForwardSocketConn) e.UserToken;
            try
            {
                do
                {
                    // No more things to receive
                    if (!HandleReceiveCompletion(e)) return;
                } while (!conn.src.ReceiveAsync(e));
            }
            catch (ObjectDisposedException)
            {
                // Probably caused by a normal cancellation from this side. Ok to ignore
            }
        }
    }
    
    public class SimpleRedisProxy
    {
        private Socket servSocket;
        private string ip;
        private int port;
        private RedisShard shard;

        public SimpleRedisProxy(string ip, int port, RedisShard shard)
        {
            this.ip = ip;
            this.port = port;
            this.shard = shard;
        }

        public void StartServer()
        {
            var ipAddr = IPAddress.Parse(ip);
            var endPoint = new IPEndPoint(ipAddr, port);
            servSocket = new Socket(ipAddr.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            servSocket.Bind(endPoint);
            servSocket.Listen(512);

            var acceptEventArg = new SocketAsyncEventArgs();
            acceptEventArg.Completed += AcceptEventArg_Completed;
            if (!servSocket.AcceptAsync(acceptEventArg))
                AcceptEventArg_Completed(null, acceptEventArg);
        }

        public void StopServer()
        {
            servSocket.Dispose();
        }

        private bool HandleNewClientConnection(SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                e.Dispose();
                return false;
            }

            var redisConn = MessageUtil.GetNewRedisConnection(shard);
            // Set up listening events on redis socket
            var redisSaea = new SocketAsyncEventArgs();
            redisSaea.SetBuffer(new byte[1 << 20], 0, 1 << 20);
            redisSaea.UserToken = new ForwardSocketConn(redisConn, e.AcceptSocket);
            redisSaea.Completed += ForwardSocketConn.RecvEventArg_Completed;
            var async = redisConn.ReceiveAsync(redisSaea);
            // Server side should not have anything available yet
            Debug.Assert(async);

            // Set up listening events on client socket
            var clientSaea = new SocketAsyncEventArgs();
            redisSaea.SetBuffer(new byte[1 << 20], 0, 1 << 20);
            clientSaea.UserToken = new ForwardSocketConn(e.AcceptSocket, redisConn);
            clientSaea.Completed += ForwardSocketConn.RecvEventArg_Completed;
            // If the client already have packets, avoid handling it here on the handler thread so we don't block future accepts.
            if (!e.AcceptSocket.ReceiveAsync(clientSaea))
                Task.Run(() => ForwardSocketConn.RecvEventArg_Completed(null, clientSaea));
            return true;
        }

        private void AcceptEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            do
            {
                if (!HandleNewClientConnection(e)) break;
                e.AcceptSocket = null;
            } while (!servSocket.AcceptAsync(e));
        }
    }
}