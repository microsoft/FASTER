using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.libdpr
{
    /// <summary>
    ///     A simple single-server DprFinder implementation relying primarily on graph traversal.
    ///     Fault-tolerant provided that the runtime environment can restart the server on the same storage volume
    ///     and IP address in bounded time (fail-restart model).
    ///     The server speaks the Redis protocol and appears as a Redis server that supports the following commands:\
    ///     AddWorker(worker) -> OK
    ///     RemoveWorker(worker) -> OK
    ///     NewCheckpoint(wv, deps) -> OK
    ///     Sync() -> state
    ///     All parameters and return values are Redis bulk strings of bytes that encode the corresponding C#
    ///     object with the exception of return values of '+OK\r\n's
    /// </summary>
    public class RespGraphDprFinderServer : IDisposable
    {
        private readonly GraphDprFinderBackend backend;
        private readonly string ip;
        private readonly int port;
        private Thread processThread;
        private Socket servSocket;
        private ManualResetEventSlim termination;

        /// <summary>
        ///     Constructs a new RespGraphDrpFinderServer instance at the given ip, listening on the given port,
        ///     and using the given backend object
        /// </summary>
        /// <param name="ip">ip address of server</param>
        /// <param name="port">port to listen on the server</param>
        /// <param name="backend">backend of the server</param>
        public RespGraphDprFinderServer(string ip, int port, GraphDprFinderBackend backend)
        {
            this.ip = ip;
            this.port = port;
            this.backend = backend;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            servSocket.Dispose();
            // TODO(Tianyu): Clean shutdown of client connections
            termination.Set();
            processThread.Join();
            backend.Dispose();
        }

        /// <summary>
        ///     Main server loop for DPR finding
        /// </summary>
        public void StartServer()
        {
            termination = new ManualResetEventSlim();

            processThread = new Thread(() =>
            {
                while (!termination.IsSet)
                    backend.Process();
            });
            processThread.Start();

            var ipAddr = IPAddress.Parse(ip);
            var endPoint = new IPEndPoint(ipAddr, port);
            servSocket = new Socket(ipAddr.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            servSocket.Bind(endPoint);
            servSocket.Listen(512);
            servSocket.NoDelay = true;

            var acceptEventArg = new SocketAsyncEventArgs();
            acceptEventArg.Completed += AcceptEventArg_Completed;
            if (!servSocket.AcceptAsync(acceptEventArg))
                AcceptEventArg_Completed(null, acceptEventArg);
        }

        private bool HandleNewClientConnection(SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                e.Dispose();
                return false;
            }

            e.AcceptSocket.NoDelay = true;
            // Set up listening events
            var saea = new SocketAsyncEventArgs();
            saea.SetBuffer(new byte[1 << 15], 0, 1 << 15);
            saea.UserToken = new DprFinderRedisProtocolConnState(e.AcceptSocket, HandleClientCommand);
            saea.Completed += DprFinderRedisProtocolConnState.RecvEventArg_Completed;
            // If the client already have packets, avoid handling it here on the handler thread so we don't block future accepts.
            if (!e.AcceptSocket.ReceiveAsync(saea))
                Task.Run(() => DprFinderRedisProtocolConnState.RecvEventArg_Completed(null, saea));
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

        private void HandleClientCommand(DprFinderCommand command, DprFinderSocketReaderWriter socket)
        {
            switch (command.commandType)
            {
                case DprFinderCommand.Type.NEW_CHECKPOINT:
                    backend.NewCheckpoint(command.worldLine, command.wv, command.deps);
                    break;
                case DprFinderCommand.Type.GRAPH_RESENT:
                    backend.MarkWorkerAccountedFor(command.wv.Worker, command.wv.Version);
                    socket.SendOk();
                    break;
                case DprFinderCommand.Type.SYNC:
                    var precomputedResponse = backend.GetPrecomputedResponse();
                    precomputedResponse.rwLatch.EnterReadLock();
                    socket.SendSyncResponse(backend.MaxVersion(),
                        ValueTuple.Create(precomputedResponse.serializedResponse, precomputedResponse.responseEnd));
                    precomputedResponse.rwLatch.ExitReadLock();
                    break;
                case DprFinderCommand.Type.ADD_WORKER:
                    backend.AddWorker(command.w, socket.SendAddWorkerResponse);
                    break;
                case DprFinderCommand.Type.DELETE_WORKER:
                    backend.DeleteWorker(command.w, socket.SendOk);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}