// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;

namespace FASTER.benchmark
{
    class Options
    {
        [Option('b', "benchmark", Required = false, Default = 0,
        HelpText = "Benchmark to run (0 = YCSB)")]
        public int Benchmark { get; set; }

        [Option('t', "threads", Required = false, Default = 8,
         HelpText = "Number of threads to run the workload on")]
        public int ThreadCount { get; set; }

        [Option('n', "numa", Required = false, Default = 0,
             HelpText = "0 = No sharding across NUMA sockets, 1 = Sharding across NUMA sockets")]
        public int NumaStyle { get; set; }

        [Option('r', "read_percent", Required = false, Default = 50,
         HelpText = "Percentage of reads (-1 for 100% read-modify-write")]
        public int ReadPercent { get; set; }

        [Option('d', "distribution", Required = false, Default = "uniform",
            HelpText = "Distribution of keys in workload")]
        public string Distribution { get; set; }

        [Option('i', "ip_address", Required = false, Default = "127.0.0.1", HelpText = "IP address of remote server")]
        public string IpAddress { get; set; }

        [Option('p', "port", Required = false, Default = 3278, HelpText = "Port of remote server")]
        public int Port { get; set; }

        [Option('s', "skipsetup", Required = false, Default = false, HelpText = "Skip setup phase of workload")]
        public bool SkipSetup { get; set; }

    }

    enum BenchmarkType : int
    {
        Ycsb, ConcurrentDictionaryYcsb
    };

    public class Program
    {
        public static void Main(string[] args)
        {
            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed)
            {
                return;
            }

            var options = result.MapResult(o => o, xs => new Options());
            BenchmarkType b = (BenchmarkType)options.Benchmark;

            if (b == BenchmarkType.Ycsb)
            {
                var test = new FASTER_YcsbBenchmark(options.ThreadCount, options.NumaStyle, options.Distribution, options.ReadPercent, options.IpAddress, options.Port, options.SkipSetup);
                test.Run();
            }
        }
    }
}
