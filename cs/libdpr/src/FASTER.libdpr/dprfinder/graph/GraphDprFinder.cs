using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace FASTER.libdpr
{
    /// <summary>
    ///     DprFinder implementation backed by a simple server component <see cref="GraphDprFinderServer" />
    ///     The server needs to be provisioned, deployed, and kept alive separately for the DprFinder to work.
    /// </summary>
    public class GraphDprFinder : IDprFinder
    {
        private readonly Socket dprFinderConn;
        private GraphDprFinderBackend.State lastKnownState;
        private long maxVersion;

        private readonly DprFinderResponseParser parser = new DprFinderResponseParser();
        private readonly byte[] recvBuffer = new byte[1 << 20];

        /// <summary>
        ///     Construct a new SimpleDprFinder client with the given socket that connects to the backend
        /// </summary>
        /// <param name="dprFinderConn"> a (connected) socket to the DPR finder backend </param>
        // TODO(Tianyu): Handle possible reconnect due to dpr finder restarts 
        public GraphDprFinder(Socket dprFinderConn)
        {
            this.dprFinderConn = dprFinderConn;
        }

        public GraphDprFinder(string ip, int port)
        {
            var ipEndpoint = new IPEndPoint(IPAddress.Parse(ip), port);
            dprFinderConn = new Socket(ipEndpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            dprFinderConn.Connect(ipEndpoint);
        }

        /// <inheritdoc />
        public long SafeVersion(Worker worker)
        {
            return lastKnownState?.GetCurrentCut()[worker] ?? 0;
        }

        /// <inheritdoc />
        public IDprStateSnapshot GetStateSnapshot()
        {
            return new DictionaryDprStateSnapshot(lastKnownState?.GetCurrentCut());
        }

        /// <inheritdoc />
        public long SystemWorldLine()
        {
            return lastKnownState?.GetCurrentWorldLines().Select(e => e.Value).Max() ?? 0;
        }

        /// <inheritdoc />
        public long GlobalMaxVersion()
        {
            return maxVersion;
        }

        /// <inheritdoc />
        public void ReportNewPersistentVersion(long worldLine, WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
        {
            lock (dprFinderConn)
            {
                dprFinderConn.SendNewCheckpointCommand(worldLine, persisted, deps);
                var received = dprFinderConn.Receive(recvBuffer);
                Debug.Assert(received == 5 && Encoding.ASCII.GetString(recvBuffer, 0, received).Equals("+OK\r\n"));
            }
        }

        /// <inheritdoc />
        public bool Refresh()
        {
            lock (dprFinderConn)
            {
                dprFinderConn.SendSyncCommand();
                ProcessRespResponse();

                maxVersion = BitConverter.ToInt64(recvBuffer, parser.stringStart);
                var newState = new GraphDprFinderBackend.State(recvBuffer, parser.stringStart + sizeof(long));
                Interlocked.Exchange(ref lastKnownState, newState);
            }

            // GraphDprFinder will never request that workers resend dependency information
            return true;
        }

        /// <inheritdoc />
        public void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion)
        {
            lock (dprFinderConn)
            {
                dprFinderConn.SendReportRecoveryCommand(latestRecoveredVersion, worldLine);
                var received = dprFinderConn.Receive(recvBuffer);
                Debug.Assert(received == 5 && Encoding.ASCII.GetString(recvBuffer, 0, received).Equals("+OK\r\n"));
            }
        }

        public long NewWorker(Worker id, IStateObject stateObject)
        {
            lock (dprFinderConn)
            {
                dprFinderConn.SendAddWorkerCommand(id);
                ProcessRespResponse();
                lastKnownState ??= new GraphDprFinderBackend.State();
                lastKnownState.GetCurrentWorldLines()[id] = BitConverter.ToInt64(recvBuffer, parser.stringStart);
                return BitConverter.ToInt64(recvBuffer, parser.stringStart + sizeof(long));
            }
        }

        public void DeleteWorker(Worker id)
        {
            lock (dprFinderConn)
            {
                dprFinderConn.SendDeleteWorkerCommand(id);
                var received = dprFinderConn.Receive(recvBuffer);
                Debug.Assert(received == 5 && Encoding.ASCII.GetString(recvBuffer, 0, received).Equals("+OK\r\n"));
            }
        }

        public void ResendGraph(Worker worker, IStateObject stateObject)
        {
            // Nothing to do here
        }

        private void ProcessRespResponse()
        {
            int i = 0, receivedSize = 0;
            while (true)
            {
                receivedSize += dprFinderConn.Receive(recvBuffer);
                for (; i < receivedSize; i++)
                    if (parser.ProcessChar(i, recvBuffer))
                        return;
            }
        }
    }
}