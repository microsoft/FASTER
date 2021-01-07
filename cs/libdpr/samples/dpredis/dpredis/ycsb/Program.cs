using System;
using CommandLine;

namespace dpredis.ycsb
{
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
    }

    class Program
    {
        static void Main(string[] args)
        {
            var result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) throw new Exception();

            var options = result.MapResult(o => o, xs => new Options());
            if (options.Type.Equals("coordinator"))
            {
                var c = new YcsbCoordinator(new BenchmarkConfiguration
                {
                    clientThreadCount = options.ClientThreadCount,
                    distribution = options.Distribution,
                    readPercent = options.ReadPercent,
                    checkpointMilli = options.CheckpointInterval, // no checkpoints
                    windowSize = options.WindowSize,
                    batchSize = options.BatchSize,
                });
                c.Run();
            }
            else if (options.Type.Equals("worker"))
            {
                var info = YcsbCoordinator.clusterConfig.GetInfo(options.WorkerId);
                if (info.type == WorkerType.PROXY)
                    new YcsbProxy(options.WorkerId).Run();
                else
                    new YcsbClient(options.WorkerId).Run(new BenchmarkConfiguration
                    {
                        clientThreadCount =  options.ClientThreadCount,
                        distribution = options.Distribution,
                        readPercent = options.ReadPercent,
                        checkpointMilli = options.CheckpointInterval, // no checkpoints
                        windowSize = options.WindowSize,
                        batchSize = options.BatchSize,
                        dprFinderIP = "10.0.1.7",
                        dprFinderPort = 15721
                    });
            }
            else
            {
                throw new Exception();
            }
        }
    }
}