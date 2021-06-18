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
    /// A DprStateSnapshot backed by a dictionary mapping from worker to version 
    /// </summary>
    public class DictionaryDprStateSnapshot : IDprStateSnapshot
    {
        private readonly Dictionary<Worker, long> dprTableSnapshot;

        /// <summary>
        /// Constructs a new DprStateSnapshot backed by the given dictionary
        /// </summary>
        /// <param name="dprTableSnapshot"> dictionary that encodes the DPR state </param>
        public DictionaryDprStateSnapshot(Dictionary<Worker, long> dprTableSnapshot)
        {
            this.dprTableSnapshot = dprTableSnapshot;
        }

        /// <inheritdoc/>
        public long SafeVersion(Worker worker)
        {
            if (dprTableSnapshot == null) return 0;
            return !dprTableSnapshot.TryGetValue(worker, out var safeVersion) ? 0 : safeVersion;
        }
    }

    /// <summary>
    /// DprFinder implementation backed by a simple server component <see cref="GraphDprFinderServer"/>
    /// The server needs to be provisioned, deployed, and kept alive separately for the DprFinder to work.
    /// </summary>
    public class GraphDprFinder : IDprFinder
    {
        private GraphDprFinderBackend.State lastKnownState;
        private long maxVersion;

        private Socket dprFinderConn;
        private byte[] recvBuffer = new byte[1 << 20];

        private DprFinderResponseParser parser = new DprFinderResponseParser();

        /// <summary>
        /// Construct a new SimpleDprFinder client with the given socket that connects to the backend
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

        /// <inheritdoc/>
        public long SafeVersion(Worker worker)
        {
            return lastKnownState?.GetCurrentCut()[worker] ?? 0;
        }

        /// <inheritdoc/>
        public IDprStateSnapshot GetStateSnapshot()
        {
            return new DictionaryDprStateSnapshot(lastKnownState?.GetCurrentCut());
        }

        /// <inheritdoc/>
        public long SystemWorldLine()
        {
            return lastKnownState?.GetCurrentWorldLines().Select(e => e.Value).Max() ?? 0;
        }

        /// <inheritdoc/>
        public long GlobalMaxVersion()
        {
            return maxVersion;
        }

        /// <inheritdoc/>
        public void ReportNewPersistentVersion(WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
        {
            lock (dprFinderConn)
            {
                dprFinderConn.SendNewCheckpointCommand(persisted, deps);
                var received = dprFinderConn.Receive(recvBuffer);
                Debug.Assert(received == 5 && Encoding.ASCII.GetString(recvBuffer, 0, received).Equals("+OK\r\n"));
            }
        }

        /// <inheritdoc/>
        public void Refresh()
        {
            lock (dprFinderConn)
            {
                dprFinderConn.SendSyncCommand();
                ProcessRespResponse();
                maxVersion = BitConverter.ToInt64(recvBuffer, parser.stringStart);
                var newState = new GraphDprFinderBackend.State(recvBuffer, parser.stringStart + sizeof(long));
                Interlocked.Exchange(ref lastKnownState, newState);
            }

        }

        private void ProcessRespResponse()
        {
            int i = 0, receivedSize = 0;
            while (true)
            {
                receivedSize += dprFinderConn.Receive(recvBuffer);
                for (; i < receivedSize; i++)
                {
                    if (parser.ProcessChar(i, recvBuffer)) return;
                }
            }
        }
        
        /// <inheritdoc/>
        public void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion)
        {
            lock (dprFinderConn)
            {
                dprFinderConn.SendReportRecoveryCommand(latestRecoveredVersion, worldLine);
                var received = dprFinderConn.Receive(recvBuffer);
                Debug.Assert(received == 5 && Encoding.ASCII.GetString(recvBuffer, 0, received).Equals("+OK\r\n"));
            }
        }

        public long NewWorker(Worker id)
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
    }
}