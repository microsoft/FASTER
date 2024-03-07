using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace FASTER.libdpr
{
    /// <summary>
    /// DPR Finder client stub that connects to a backend RespGraphDprFinderServer
    /// </summary>
    public sealed class RespGraphDprFinder : IDprFinder, IDisposable
    {
        private readonly DprFinderSocketReaderWriter socket;
        private readonly Dictionary<WorkerId, long> lastKnownCut = new Dictionary<WorkerId, long>();
        private readonly DprFinderResponseParser parser = new DprFinderResponseParser();
        private readonly byte[] recvBuffer = new byte[1 << 20];
        private ClusterState lastKnownClusterState;
        private long maxVersion;
        
        /// <summary>
        /// Create a new DprFinder using the supplied socket
        /// </summary>
        /// <param name="dprFinderConn"> socket to use</param>
        public RespGraphDprFinder(Socket dprFinderConn)
        {
            dprFinderConn.NoDelay = true;
            socket = new DprFinderSocketReaderWriter(dprFinderConn);
        }

        /// <summary>
        /// Create a new DprFinder by connecting to the given endpoint
        /// </summary>
        /// <param name="ip">IP address of the desired endpoint</param>
        /// <param name="port">port of the desired endpoint</param>
        public RespGraphDprFinder(string ip, int port)
        {
            var ipEndpoint = new IPEndPoint(IPAddress.Parse(ip), port);
            var conn = new Socket(ipEndpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            conn.NoDelay = true;
            conn.Connect(ipEndpoint);
            socket = new DprFinderSocketReaderWriter(conn);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            socket.Dispose();
        }

        /// <inheritdoc />
        public long SafeVersion(WorkerId workerId)
        {
            return lastKnownCut.TryGetValue(workerId, out var result) ? result : 0;
        }

        /// <inheritdoc/>
        public IDprStateSnapshot GetStateSnapshot()
        {
            return new DictionaryDprStateSnapshot(lastKnownCut, SystemWorldLine());
        }

        /// <inheritdoc/>
        public long SystemWorldLine()
        {
            return lastKnownClusterState?.currentWorldLine ?? 1;
        }

        /// <inheritdoc/>
        public long GlobalMaxVersion()
        {
            return maxVersion;
        }

        /// <inheritdoc/>
        public void ReportNewPersistentVersion(long worldLine, WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
        {
            lock (socket)
                socket.SendNewCheckpointCommand(worldLine, persisted, deps);
        }

        /// <inheritdoc/>
        public bool Refresh()
        {
            lock (socket)
            {
                socket.SendSyncCommand();
                ProcessRespResponse();

                maxVersion = BitConverter.ToInt64(recvBuffer, parser.stringStart);
                var newState = ClusterState.FromBuffer(recvBuffer, parser.stringStart + sizeof(long), out var head);
                Interlocked.Exchange(ref lastKnownClusterState, newState);
                // Cut is unavailable, signal a resend.
                if (BitConverter.ToInt32(recvBuffer, head) == -1) return false;
                lock (lastKnownCut)
                {
                    RespUtil.ReadDictionaryFromBytes(recvBuffer, head, lastKnownCut);
                }
            }

            return true;
        }
        
        /// <inheritdoc/>
        public void ResendGraph(WorkerId workerId, IStateObject stateObject)
        {
            lock (socket)
            {
                var acks = socket.SendGraphReconstruction(workerId, stateObject);
                socket.WaitForAcks(acks);
            }
        }
        
        /// <inheritdoc/>
        public long NewWorker(WorkerId id, IStateObject stateObject)
        {
            if (stateObject != null)
                ResendGraph(id, stateObject);
            lock (socket)
            {
                socket.SendAddWorkerCommand(id);
                ProcessRespResponse();
                lastKnownClusterState ??= new ClusterState();
                lastKnownClusterState.currentWorldLine = BitConverter.ToInt64(recvBuffer, parser.stringStart);
                return BitConverter.ToInt64(recvBuffer, parser.stringStart + sizeof(long));
            }
        }

        /// <inheritdoc/>
        public void DeleteWorker(WorkerId id)
        {
            lock (socket)
            {
                socket.SendDeleteWorkerCommand(id);
                socket.WaitForAcks(1);
            }
        }

        private void ProcessRespResponse()
        {
            int i = 0, receivedSize = 0;
            while (true)
            {
                receivedSize += socket.ReceiveInto(recvBuffer);
                for (; i < receivedSize; i++)
                    if (parser.ProcessChar(i, recvBuffer))
                        return;
            }
        }
    }
}