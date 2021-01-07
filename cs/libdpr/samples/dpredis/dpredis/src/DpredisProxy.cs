using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using FASTER.libdpr;

namespace dpredis
{
    internal class ProxyClientConnState : MessageUtil.AbstractDprConnState
    {
        private RedisMiddleMan middleMan;

        public ProxyClientConnState(RedisMiddleMan middleMan)
        {
            this.middleMan = middleMan;
        }

        protected override void HandleMessage(byte[] buf, int offset, int size)
        {
            middleMan.HandleNewRequest(buf, offset, size);
        }
    }

    internal class ProxyRedisConnState : MessageUtil.AbstractRedisConnState
    {
        private RedisMiddleMan middleMan;

        public ProxyRedisConnState(RedisMiddleMan middleMan)
        {
            this.middleMan = middleMan;
        }
        protected override bool HandleSimpleString(byte[] buf, int start, int end)
        {
            return middleMan.HandleNewResponse(buf, end);
        }
    }

    public class DpredisProxy
    {
        private const int batchMaxSize = 1 << 20;
        private Socket servSocket;
        private string ip;
        private int port;
        private DprServer<RedisStateObject, long> dprServer;
        private SimpleObjectPool<DpredisBatchHandle> batchHandlePool;

        private ConcurrentDictionary<RedisMiddleMan, object> middleMen;

        public DpredisProxy(string ip, int port, DprServer<RedisStateObject, long> dprServer)
        {
            this.ip = ip;
            this.port = port;
            this.dprServer = dprServer;
            batchHandlePool = new SimpleObjectPool<DpredisBatchHandle>(() => new DpredisBatchHandle());
            middleMen = new ConcurrentDictionary<RedisMiddleMan, object>();
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
            foreach (var middleMan in middleMen.Keys)
                middleMan.Dispose();
        }

        private bool HandleNewClientConnection(SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                e.Dispose();
                return false;
            }

            // Establish a redis connection for every client connection
            var redisConn = dprServer.StateObject().GetNewRedisConnection();
            var middleMan = new RedisMiddleMan(e.AcceptSocket, redisConn, batchHandlePool, dprServer);
            middleMen.TryAdd(middleMan, null);
            // Set up listening events on redis socket
            var redisSaea = new SocketAsyncEventArgs();
            redisSaea.SetBuffer(new byte[batchMaxSize], 0, batchMaxSize);
            redisSaea.UserToken = new ProxyRedisConnState(middleMan);
            redisSaea.Completed += MessageUtil.AbstractRedisConnState.RecvEventArg_Completed;
            var bytesAvailable = redisConn.ReceiveAsync(redisSaea);
            // Server side should not have anything available yet
            Debug.Assert(!bytesAvailable);
            
            // Set up listening events on client socket
            var clientSaea = new SocketAsyncEventArgs();
            clientSaea.SetBuffer(new byte[batchMaxSize], 0, batchMaxSize);
            clientSaea.UserToken = new ProxyClientConnState(middleMan);
            clientSaea.Completed += MessageUtil.AbstractDprConnState.RecvEventArg_Completed;
            e.AcceptSocket.NoDelay = true;
            // If the client already have packets, avoid handling it here on the handler thread so we don't block future accepts.
            if (e.AcceptSocket.ReceiveAsync(clientSaea))
            {
                Task.Run(() =>  MessageUtil.AbstractDprConnState.RecvEventArg_Completed(null, clientSaea));
            }
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