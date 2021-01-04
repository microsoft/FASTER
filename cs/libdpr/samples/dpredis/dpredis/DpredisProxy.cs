using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using FASTER.libdpr;

namespace dpredis
{


    internal class DpredisServerInConnState : MessageUtil.AbstractConnState
    {
        private DpredisProxy proxy;

        public DpredisServerInConnState(Socket socket, DpredisProxy proxy)
        {
            Reset(socket);
            this.proxy = proxy;
        }
        protected override unsafe void HandleMessage(byte[] buf, int offset, int size)
        {
            fixed (byte* b = buf)
            {
                ref var header = ref Unsafe.AsRef<DprBatchRequestHeader>(b + offset);b
            }
        }
    }

    // internal class DpredisServerOutConnState : MessageUtil.AbstractConnState
    // {
    //     
    // }
    
    public class DpredisProxy : IDisposable
    {
        private const int batchMaxSize = 1 << 20;
        private Socket servSocket;
        private ThreadLocal<Socket> redisSocket;
        private string ip;
        private int port;
        internal DprManager<SimpleStateObjectAdapter<RedisStateObject, long>, long> dprManager;
        
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