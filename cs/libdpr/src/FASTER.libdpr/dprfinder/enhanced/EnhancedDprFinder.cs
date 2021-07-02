using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text;

namespace FASTER.libdpr
{
    public class EnhancedDprFinder : IDprFinder
    {
        private Dictionary<Worker, long> lastKnownCut;
        private ClusterState lastKnownClusterState;
        private long maxVersion;

        private Socket dprFinderConn;
        private byte[] recvBuffer = new byte[1 << 20];
        private DprFinderResponseParser parser = new DprFinderResponseParser();

        private IStateObject stateObject;

        public EnhancedDprFinder(Socket dprFinderConn, IStateObject stateObject)
        {
            this.dprFinderConn = dprFinderConn;
            this.stateObject = stateObject;
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
                dprFinderConn.SendNewCheckpointCommand(persisted, deps);
                var received = dprFinderConn.Receive(recvBuffer);
                Debug.Assert(received == 5 && Encoding.ASCII.GetString(recvBuffer, 0, received).Equals("+OK\r\n"));
            }        
        }

        public void Refresh()
        {
            lock (dprFinderConn)
            {
                dprFinderConn.SendSyncCommand();
                ProcessRespResponse();

                // TODO(Tianyu): deserialize response and trigger a send of all local state if necessary
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

        public void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion)
        {
            // No need to report recovery for enhanced DprFinder
        }

        private void SendAllLocalVersions(long worldLine)
        {
            
        }

        public long NewWorker(Worker id)
        {
            throw new System.NotImplementedException();
        }

        public void DeleteWorker(Worker id)
        {
            throw new System.NotImplementedException();
        }
    }
}