using System;
using System.Threading;
using CommandLine;
using FASTER.libdpr;

namespace dpredis.ycsb
{
    public class BenchmarkConsts
    {
#if DEBUG
        public const bool kUseSmallData = true;
        public const bool kUseSyntheticData = true;
#else
        public const bool kUseSmallData = false;
        public const bool kUseSyntheticData = false;
#endif
        public const long kInitCount = kUseSmallData ? 2500480 : 250000000;
        public const long kTxnCount = kUseSmallData ? 10000000 : 1000000000;
        public const int kMaxKey = kUseSmallData ? 1 << 22 : 1 << 28;

        public const int kFileChunkSize = 4096;
        public const long kChunkSize = 640;
        public const int kRunSeconds = 30;

        public const int kWorkerIdBits = 3;

        public const bool kCollectLatency = true;
        public const bool kTriggerRecovery = false;
    }
    
    class Options
    {
        [Option('t', "type", Required = true, HelpText = "worker or coordinator")]
        public string Type { get; set; }

        [Option('c', "client-threads", Required = false, Default = 8,
            HelpText = "Number of threads to run the workload on in the client")]
        public int ClientThreadCount { get; set; }

        [Option('b', "batch-size", Required = false, Default = 1024,
            HelpText = "Number of requests to batch per client before sending to the server")]
        public int BatchSize { get; set; }

        [Option('d', "distribution", Required = false, Default = "uniform",
            HelpText = "Distribution of keys in workload")]
        public string Distribution { get; set; }

        [Option('r', "read_percent", Required = false, Default = 50,
            HelpText = "Percentage of reads (-1 for 100% read-modify-write")]
        public int ReadPercent { get; set; }

        [Option('n', "worker_id", Required = false, Default = 0)]
        public int WorkerId { get; set; }

        [Option('i', "checkpoint_interval", Required = false, Default = -1)]
        public int CheckpointInterval { get; set; }

        [Option('w', "window_size", Required = false, Default = 4096)]
        public int WindowSize { get; set; }
        
        [Option('l', "load_data", Required = false, Default = false)]
        public bool LoadDatabase { get; set; }
        
        [Option('p', "proxy", Required = false, Default = "none")]
        public string ProxyType { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            YcsbCoordinator.clusterConfig = new ClusterConfiguration();
            // TODO(Tianyu): Example config
            YcsbCoordinator.clusterConfig               
                .AddProxy("10.0.1.8", 15721, new RedisShard {ip = "10.0.1.8", port = 6379})
                .AddProxy("10.0.1.9", 15721, new RedisShard {ip = "10.0.1.9", port = 6379})
                .AddProxy("10.0.1.11", 15721, new RedisShard {ip = "10.0.1.11", port = 6379})
                .AddProxy("10.0.1.10", 15721, new RedisShard {ip = "10.0.1.10", port = 6379})
                .AddProxy("10.0.1.12", 15721, new RedisShard {ip = "10.0.1.12", port = 6379})
                .AddProxy("10.0.1.13", 15721, new RedisShard {ip = "10.0.1.13", port = 6379})
                .AddProxy("10.0.1.14", 15721, new RedisShard {ip = "10.0.1.14", port = 6379})
                .AddProxy("10.0.1.15", 15721, new RedisShard {ip = "10.0.1.15", port = 6379})
                .AddClient("10.0.1.16", 15721)
                .AddClient("10.0.1.17", 15721)
                .AddClient("10.0.1.18", 15721)
                .AddClient("10.0.1.19", 15721)
                .AddClient("10.0.1.20", 15721)
                .AddClient("10.0.1.21", 15721)
                .AddClient("10.0.1.22", 15721)
                .AddClient("10.0.1.23", 15721);
            
            var result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) throw new Exception();

            var options = result.MapResult(o => o, xs => new Options());
            if (options.Type.Equals("coordinator"))
            {
                var dprFinder = new TestDprFinderServer();

                dprFinder.StartServer("10.0.1.7", 15445);
                var c = new YcsbCoordinator(new BenchmarkConfiguration
                {
                    clientThreadCount = options.ClientThreadCount,
                    distribution = options.Distribution,
                    readPercent = options.ReadPercent,
                    checkpointMilli = options.CheckpointInterval, // no checkpoints
                    windowSize = options.WindowSize,
                    batchSize = options.BatchSize,
                    dprFinderIP = "10.0.1.7",
                    dprFinderPort = 15445,
                    load = options.LoadDatabase,
                    proxyType = options.ProxyType
                    
                });
                c.Run();
                dprFinder.EndServer();
            }
            else if (options.Type.Equals("worker"))
            {
                var info = YcsbCoordinator.clusterConfig.GetInfo(options.WorkerId);
                if (info.type == WorkerType.PROXY)
                    new YcsbProxy(options.WorkerId).Run();
                else
                    new YcsbClient(options.WorkerId).Run();
            }
            else
            {
                throw new Exception();
            }
        }
    }
}