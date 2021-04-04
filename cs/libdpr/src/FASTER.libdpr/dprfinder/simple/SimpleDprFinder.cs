using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace FASTER.libdpr
{
    public class DictionaryDprTableSnapshot : IDprTableSnapshot
    {
        private readonly Dictionary<Worker, long> cprTableSnapshot;

        public DictionaryDprTableSnapshot(Dictionary<Worker, long> cprTableSnapshot)
        {
            this.cprTableSnapshot = cprTableSnapshot;
        }

        public long SafeVersion(Worker worker)
        {
            return !cprTableSnapshot.TryGetValue(worker, out var safeVersion) ? 0 : safeVersion;
        }
    }

    public class SimpleDprFinder : IDprFinder
    {
        private readonly Worker me;
        private SimpleDprFinderBackend.State lastKnownState;
        private long maxVersion;

        private Socket dprFinderConn;
        private byte[] recvBuffer = new byte[1 << 20];

        private DprFinderResponseParser parser = new DprFinderResponseParser();

        public long SafeVersion(Worker worker)
        {
            return lastKnownState.cut[worker];
        }

        public IDprTableSnapshot ReadSnapshot()
        {
            return new DictionaryDprTableSnapshot(lastKnownState.cut);
        }

        public long SystemWorldLine()
        {
            return lastKnownState.worldLines[Worker.CLUSTER_MANAGER];
        }

        public long GlobalMaxVersion()
        {
            return maxVersion;
        }

        public void ReportNewPersistentVersion(WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
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
                ProcessSyncResponse();
                maxVersion = BitConverter.ToInt64(recvBuffer, parser.stringStart);
                var newState = new SimpleDprFinderBackend.State(recvBuffer, parser.stringStart + sizeof(long));
                Interlocked.Exchange(ref lastKnownState, newState);
            }

        }

        private void ProcessSyncResponse()
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
            lock (dprFinderConn)
            {
                dprFinderConn.SendReportRecoveryCommand(latestRecoveredVersion, worldLine);
                var received = dprFinderConn.Receive(recvBuffer);
                Debug.Assert(received == 5 && Encoding.ASCII.GetString(recvBuffer, 0, received).Equals("+OK\r\n"));
            }
        }
    }
}