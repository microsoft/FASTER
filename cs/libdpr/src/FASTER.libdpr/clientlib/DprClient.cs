using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.libdpr
{
    // TODO(Tianyu): Document
    public class DprClient
    {
        private IDprFinder dprFinder;
        private long dprViewNumber = 0;
        private ConcurrentDictionary<Guid, DprClientSession> sessions;
        
        private Thread refreshThread;
        private readonly long refreshMilli;
        private ManualResetEventSlim termination;

        public DprClient(IDprFinder dprFinder, long refreshMilli = 50)
        {
            this.dprFinder = dprFinder;
            sessions = new ConcurrentDictionary<Guid, DprClientSession>();
            this.refreshMilli = refreshMilli;
            termination = new ManualResetEventSlim();
        }
        
        // TODO(Tianyu): Change to not start thread
        public void Start()
        {
            termination = new ManualResetEventSlim();
            refreshThread = new Thread(async () =>
            {
                while (!termination.IsSet)
                {
                    await Task.Delay((int) refreshMilli);
                    dprViewNumber++;
                    dprFinder.Refresh();
                }
            });
            refreshThread.Start();
        }

        public void End()
        {
            termination.Set();
            refreshThread.Join();
        }

        internal IDprFinder GetDprFinder() => dprFinder;

        internal long GetDprViewNumber() => dprViewNumber;

        public DprClientSession GetSession(Guid guid, bool trackCommits)
        {
            return sessions.GetOrAdd(guid, id => new DprClientSession(id, this, trackCommits));
        }
    }
    
}