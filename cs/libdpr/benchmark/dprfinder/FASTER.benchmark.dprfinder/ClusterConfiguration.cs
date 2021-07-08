using System.Collections.Concurrent;
using System.Collections.Generic;
using FASTER.libdpr;

namespace FASTER.benchmark
{
    public class WorkerInfo
    {
        public Worker worker;
        public string ip;
        public int port;

        public WorkerInfo(Worker worker, string ip, int port)
        {
            this.worker = worker;
            this.ip = ip;
            this.port = port;
        }
    }
    
    public class ClusterConfiguration
    {
        internal List<WorkerInfo> pods = new List<WorkerInfo>();

        public ClusterConfiguration AddWorker(string ip, int port)
        {
            var info = new WorkerInfo(new Worker(pods.Count), ip, port);
            pods.Add(info);
            return this;
        }

        public WorkerInfo GetInfoForId(int id)
        {
            return pods[id];
        }
        
        // TODO(Tianyu): Populate this from the DPR table instead of hard-coded
        public ConcurrentDictionary<Worker, WorkerInfo> GetRoutingTable()
        {
            var result = new ConcurrentDictionary<Worker, WorkerInfo>();
            foreach (var worker in pods)
                result.TryAdd(worker.worker, worker);
            return result;
        }
    }
}