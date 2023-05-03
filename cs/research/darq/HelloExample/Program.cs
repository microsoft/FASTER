using CommandLine;
using FASTER.client;
using FASTER.core;
using FASTER.darq;
using FASTER.libdpr;
using FASTER.server;

namespace HelloExample;

public class Options
{
    [Option('p', "numProcessor", Required = true,
        HelpText = "number of workers to launch")]
    public int NumProcessors { get; set; }

    [Option('n', "numGrettings", Required = true,
        HelpText = "Number of greetings to execute")]
    public int NumGreetings { get; set; }
}

public class Program
{
    private static void RunDarqWithProcessor(WorkerId me, IDarqClusterInfo clusterInfo)
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
                DeleteOnClose = false
            },
            commitIntervalMilli = 5,
            refreshIntervalMilli = 5
        });
        darqServer.Start();
        var processorClient = new ColocatedDarqProcessorClient(darqServer.GetDarq());
        processorClient.StartProcessingAsync(new HelloTaskProcessor(me, clusterInfo)).GetAwaiter().GetResult();
        darqServer.Dispose();
    }

    public static void Main(string[] args)
    {
        ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
        if (result.Tag == ParserResultType.NotParsed) return;
        var options = result.MapResult(o => o, xs => new Options());

        // Compose cluster architecture
        var clusterInfo = new HardCodedClusterInfo();
        var threads = new List<Thread>();
        for (var i = 0; i < options.NumProcessors; i++)
        {
            clusterInfo.AddWorker(new WorkerId(i), $"Test Worker {i}", "127.0.0.1", 15721 + i);
            var i1 = i;
            threads.Add(new Thread(() =>
            {
                RunDarqWithProcessor(new WorkerId(i1), clusterInfo);
            }));
        }

        foreach (var t in threads)
            t.Start();

        var darqClient = new DarqProducerClient(clusterInfo);
        // TODO(Tianyu): Implement string variant for these things
        darqClient.EnqueueMessageAsync(new WorkerId(0), );
        
        foreach (var t in threads)
            t.Join();
    }
}