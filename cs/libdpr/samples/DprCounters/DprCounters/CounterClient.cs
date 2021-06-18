using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using FASTER.libdpr;

namespace DprCounters
{
    /// <summary>
    /// Client to a cluster of CounterServers. DPR-capable. 
    /// </summary>
    public class CounterClient
    {
        private DprClient client;
        private Dictionary<Worker, IPEndPoint> cluster;
        private Thread backgroundThread;

        public CounterClient(IDprFinder dprFinder, Dictionary<Worker, IPEndPoint> cluster)
        {
            client = new DprClient(dprFinder);
            this.cluster = cluster;
        }
        
        public CounterClientSession GetSession()
        {
            return new CounterClientSession(client.GetSession(Guid.NewGuid()), cluster);
        }

        public void RefreshDpr()
        {
            client.RefreshDprView();
        }
    }
}