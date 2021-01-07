using System;
using System.Collections.Generic;
using FASTER.libdpr;

namespace dpredis.ycsb
{
    [Serializable]
    public class BenchmarkConfiguration
    {
        public int clientThreadCount;
        public string distribution;
        public int readPercent;
        public int checkpointMilli;
        public int windowSize;
        public int batchSize;
        public string dprFinderIP;
        public int dprFinderPort;

        public override string ToString()
        {
            return $"{nameof(clientThreadCount)}: {clientThreadCount}," +
                   $"{nameof(distribution)}: {distribution}," +
                   $"{nameof(readPercent)}: {readPercent}," +
                   $"{nameof(checkpointMilli)}: {checkpointMilli}," +
                   $"{nameof(windowSize)}: {windowSize}," +
                   $"{nameof(batchSize)}: {batchSize}";
        }
    }

    public enum WorkerType
    {
        PROXY, CLIENT
    }
    public struct ClusterWorker
    {
        public WorkerType type;
        public long id;
        public string ip;
        public int port;
        public RedisShard redisBackend;
    }
    
    public class ClusterConfiguration
    {
        public List<ClusterWorker> workers = new List<ClusterWorker>();

        public ClusterConfiguration AddClient(string ip, int port)
        {
            workers.Add(new ClusterWorker {type = WorkerType.CLIENT, id = workers.Count, ip = ip, port = port});
            return this;
        }

        public ClusterConfiguration AddProxy(string ip, int port, RedisShard redisBackend)
        {
            workers.Add(new ClusterWorker{type = WorkerType.PROXY, id = workers.Count, ip = ip, port = port, redisBackend = redisBackend});
            return this;
        }

        public ClusterWorker GetInfo(int id) => workers[id];
    }

    public class YcsbCoordinator
    {
        public static ClusterConfiguration clusterConfig;

        static YcsbCoordinator()
        {
            clusterConfig = new ClusterConfiguration();
            // TODO(Tianyu): Example config
            clusterConfig.AddProxy("10.0.1.8", 15721, new RedisShard {name = "", port = 6379, auth = ""})
                .AddClient("10.0.1.9", 15721);
        }

        private BenchmarkConfiguration config;

        public YcsbCoordinator(BenchmarkConfiguration config)
        {
            this.config = config;
        }
        
        public void Run() {}
    }
}