using System.Collections.Generic;
using System.Net.Sockets;

namespace FASTER.libdpr
{
    public class SimpleDprFinder : IDprFinder
    {
        private readonly Worker me;
        private Dictionary<Worker, long> recoverableCut;
        private long systemWorldLine;
        private Socket dprFinderConn;
        private byte[] buffer = new byte[1 << 15];
        
        public long SafeVersion(Worker worker)
        {
            throw new System.NotImplementedException();
        }

        public IDprTableSnapshot ReadSnapshot()
        {
            throw new System.NotImplementedException();
        }

        public long SystemWorldLine()
        {
            throw new System.NotImplementedException();
        }

        public long GlobalMaxVersion()
        {
            throw new System.NotImplementedException();
        }

        public void ReportNewPersistentVersion(WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
        {
            throw new System.NotImplementedException();
        }

        public void Refresh()
        {
            throw new System.NotImplementedException();
        }

        public void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion)
        {
            throw new System.NotImplementedException();
        }
    }
}