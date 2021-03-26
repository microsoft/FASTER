using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using FASTER.core;

namespace FASTER.libdpr
{
    public class SimpleDprFinderServer
    {
        private string ip;
        private int port;
        private Socket servSocket;

        private IDevice persistentStorage;
        private Dictionary<WorkerVersion, List<WorkerVersion>> precedenceGraph;
        private Dictionary<Worker, long> persistedCut;

        public SimpleDprFinderServer(string ip, int port, IDevice persistentStorage)
        {
            this.ip = ip;
            this.port = port;
            this.persistentStorage = persistentStorage;
            precedenceGraph = new Dictionary<WorkerVersion, List<WorkerVersion>>();
            persistedCut = new Dictionary<Worker, long>();
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
            // TODO(Tianyu): Clean shutdown of client connections
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
                    
                    break;
                case DprFinderCommand.Type.REPORT_RECOVERY:
                    break;
                case DprFinderCommand.Type.SYNC:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
        
    }
}