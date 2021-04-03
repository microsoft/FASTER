using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Resources;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;

namespace FASTER.libdpr
{
    public class SimpleDprFinderServer : IDisposable
    {
        private static readonly byte[] OkResponse = Encoding.GetEncoding("ASCII").GetBytes("+OK\r\n");
        private readonly string ip;
        private readonly int port;
        private Socket servSocket;

        private readonly SimpleDprFinderBackend backend;
        private ManualResetEventSlim termination;
        private Thread ioThread, processThread;

        public SimpleDprFinderServer(string ip, int port, SimpleDprFinderBackend backend)
        {
            this.ip = ip;
            this.port = port;
            this.backend = backend;
        }

        public void StartServer()
        {
            termination = new ManualResetEventSlim();
            ioThread = new Thread(() =>
            {
                while (!termination.IsSet)
                {
                    // TODO(Tianyu): Need to throttle/tune I/O frequency?
                    backend.PersistState();
                }
            });

            processThread = new Thread(() =>
            {
                while (!termination.IsSet)
                {
                    backend.TryFindDprCut();
                }
            });
            
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
        
        public void Dispose()
        {
            servSocket.Dispose();
            // TODO(Tianyu): Clean shutdown of client connections

            termination.Set();
            ioThread.Join();
            processThread.Join();
            backend.Dispose();
        }
        
        private bool HandleNewClientConnection(SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                e.Dispose();
                return false;
            }

            // Set up listening events
            var saea = new SocketAsyncEventArgs();
            saea.SetBuffer(new byte[BatchInfo.MaxHeaderSize], 0, BatchInfo.MaxHeaderSize);
            // TODO(Tianyu): Fill in
            saea.UserToken = new MessageUtil.DprFinderRedisProtocolConnState(e.AcceptSocket, (c, s) => { });
            saea.Completed += MessageUtil.DprFinderRedisProtocolConnState.RecvEventArg_Completed;
            // If the client already have packets, avoid handling it here on the handler thread so we don't block future accepts.
            if (!e.AcceptSocket.ReceiveAsync(saea))
                Task.Run(() =>  MessageUtil.DprFinderRedisProtocolConnState.RecvEventArg_Completed(null, saea));
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

        private void HandleClientCommand(DprFinderCommand command, Socket socket)
        {
            switch (command.commandType)
            {
                case DprFinderCommand.Type.NEW_CHECKPOINT:
                    backend.NewCheckpoint(command.wv, command.deps);
                    // Ack immediately as the graph is not required to be fault-tolerant
                    socket.Send(OkResponse);
                    break;
                case DprFinderCommand.Type.REPORT_RECOVERY:
                    // Can only send ack after recovery has been logged and fault-tolerant
                    backend.ReportRecovery(command.wv, command.worldLine, () => socket.Send(OkResponse));
                    break;
                case DprFinderCommand.Type.SYNC:
                    socket.SendSyncResponse(backend.MaxVersion(), backend.GetPersistentState());
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
        
    }
}