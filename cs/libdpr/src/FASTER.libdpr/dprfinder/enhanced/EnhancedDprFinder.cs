using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace FASTER.libdpr
{
    public class EnhancedDprFinder : IDprFinder
    {
        private Dictionary<Worker, long> lastKnownCut = new Dictionary<Worker, long>();
        private ClusterState lastKnownClusterState;
        private long maxVersion;

        private Socket dprFinderConn;
        private byte[] recvBuffer = new byte[1 << 20];
        private DprFinderResponseParser parser = new DprFinderResponseParser();
        
        public EnhancedDprFinder(Socket dprFinderConn)
        {
            this.dprFinderConn = dprFinderConn;
        }
        
        
        public long SafeVersion(Worker worker)
        {
            return lastKnownCut?[worker] ?? 0;
        }

        public IDprStateSnapshot GetStateSnapshot()
        {
            return new DictionaryDprStateSnapshot(lastKnownCut);
        }

        public long SystemWorldLine()
        {
            return lastKnownClusterState?.currentWorldLine ?? 0;
        }

        public long GlobalMaxVersion()
        {
            return maxVersion;
        }

        public void ReportNewPersistentVersion(long worldLine, WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
        {
            lock (dprFinderConn)
            {
                dprFinderConn.SendNewCheckpointCommand(worldLine, persisted, deps);
                var received = dprFinderConn.Receive(recvBuffer);
                Debug.Assert(received == 5 && Encoding.ASCII.GetString(recvBuffer, 0, received).Equals("+OK\r\n"));
            }        
        }

        public bool Refresh()
        {
            lock (dprFinderConn)
            {
                dprFinderConn.SendSyncCommand();
                ProcessRespResponse();
                
                maxVersion = BitConverter.ToInt64(recvBuffer, parser.stringStart);
                var newState = ClusterState.FromBuffer(recvBuffer, parser.stringStart + sizeof(long), out var head);
                Interlocked.Exchange(ref lastKnownClusterState, newState);
                // Cut is unavailable, signal a resend.
                if (BitConverter.ToInt32(recvBuffer, head) == -1) return false;
                RespUtil.ReadDictionaryFromBytes(recvBuffer, head, lastKnownCut);
            }

            return true;
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

        public void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion)
        {
            // No need to report recovery for enhanced DprFinder
        }

        public void ResendGraph(Worker worker, IStateObject stateObject)
        {
            lock (dprFinderConn)
            {
                var acks = dprFinderConn.SendGraphReconstruction(worker, stateObject);
                // Wait for all of the sent commands to be acked
                var received = 0;
                while (received < acks * 5)
                    received += dprFinderConn.Receive(recvBuffer);
            }
        }
        
        public long NewWorker(Worker id, IStateObject stateObject)
        {
            ResendGraph(id, stateObject);
            lock (dprFinderConn)
            {
                dprFinderConn.SendAddWorkerCommand(id);
                ProcessRespResponse();
                lastKnownClusterState ??= new ClusterState();
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