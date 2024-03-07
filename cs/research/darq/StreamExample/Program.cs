using CommandLine;
using FASTER.client;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.server;
using SimpleStream.searchlist;

namespace SimpleStream
{
    public class Options
    {
        [Option('p', "type", Required = true,
            HelpText = "type of worker to launch")]
        public string Type { get; set; }
        
        [Option('t', "trace-file", Required = true,
            HelpText = "Trace to execute")]
        public string TraceFile { get; set; }
    }

    public class Program
    {
        private static void RunStreamProducer(string traceFile, HardCodedClusterInfo clusterInfo)
        {
            var loader = new SearchListDataLoader(traceFile, clusterInfo);
            loader.LoadData();
            loader.Run(new WorkerId(0));
        }

        private static void RunDarqWithProcessor(WorkerId me, HardCodedClusterInfo clusterInfo,
            IDarqProcessor processor, bool remoteProcessor = false)
        {
            var logDevice = new LocalStorageDevice($"D:\\w{me.guid}\\data.log", deleteOnClose: true);
            var darqServer = new DarqServer(new DarqServerOptions
            {
                Port = 15721 + (int)me.guid,
                Address = "127.0.0.1",
                ClusterInfo = clusterInfo,
                me = me,
                DarqSettings = new DarqSettings
                {
                    DprFinder = default,
                    LogDevice = logDevice,
                    PageSize = 1L << 22,
                    MemorySize = 1L << 23,
                    SegmentSize = 1L << 30,
                    LogCommitManager = default,
                    LogCommitDir = default,
                    LogChecksum = LogChecksumType.None,
                    MutableFraction = default,
                    FastCommitMode = true,
                    DeleteOnClose = true
                },
                commitIntervalMilli = 5,
                refreshIntervalMilli = 5
            });
            darqServer.Start();
            IDarqProcessorClient processorClient;
            if (remoteProcessor)
                processorClient = new DarqProcessorClient("127.0.0.1", 15721 + (int)me.guid);
            else
                processorClient = new ColocatedDarqProcessorClient(darqServer.GetDarq());
            
            processorClient.StartProcessingAsync(processor).GetAwaiter().GetResult();
            
            darqServer.Dispose();
        }

        public static void Main(string[] args)
        {
            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) return;
            var options = result.MapResult(o => o, xs => new Options());
            if (options.Type.Equals("generate"))
            {
                // Change the parameters as needed
                new SearchListDataGenerator().SetOutputFile(options.TraceFile)
                    .SetSearchTermRelevantProb(0.2)
                    .SetTrendParameters(0.1, 50000, 10000)
                    .SetSearchTermLength(80)
                    .SetThroughput(50000)
                    .SetNumSearchTerms(50000 * 30)
                    .Generate();
                return;
            }

            // Compose cluster architecture
            var clusterInfo = new HardCodedClusterInfo();
            for (var i = 0; i < 4; i++)
                clusterInfo.AddWorker(new WorkerId(i), $"Test Worker {i}", "127.0.0.1", 15721 + i);

            switch (options.Type)
            {
                case "producer":
                    RunStreamProducer(options.TraceFile, clusterInfo);
                    break;
                case "preprocessor":
                    RunDarqWithProcessor(new WorkerId(0), clusterInfo, new FilterAndMapStreamProcessor(new WorkerId(0), new WorkerId(1)));
                    break;
                case "aggregator":
                    RunDarqWithProcessor(new WorkerId(1), clusterInfo, new AggregateStreamProcessor(new WorkerId(1), new WorkerId(2)));
                    break;
                case "detector":
                    RunDarqWithProcessor(new WorkerId(2), clusterInfo, new TrendDetectionStreamProcessor(new WorkerId(2)));
                    break;
            }
        }
    }
}