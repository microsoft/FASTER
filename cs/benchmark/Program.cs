// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;
using System;

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

        [Option('k', "backup", Required = false, Default = 0,
             HelpText = "Enable Backup and Restore of FasterKV for fast test startup:\n0 = None\n1 = Recover FasterKV from Checkpoint\n2 = Checkpoint FasterKV if it was not Recovered\n3 = both")]
        public int Backup { get; set; }

        [Option('r', "read_percent", Required = false, Default = 50,
         HelpText = "Percentage of reads (-1 for 100% read-modify-write")]
        public int ReadPercent { get; set; }

        [Option('d', "distribution", Required = false, Default = "uniform",
            HelpText = "Distribution of keys in workload")]
        public string Distribution { get; set; }
    }

    enum BenchmarkType : int
    {
        Ycsb, ConcurrentDictionaryYcsb
    };

    [Flags] enum BackupMode : int
    {
        None, Restore, Backukp, Both
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
                var test = new FASTER_YcsbBenchmark(options.ThreadCount, options.NumaStyle, options.Distribution, options.ReadPercent, options.Backup);
                test.Run();
            }
            else if (b == BenchmarkType.ConcurrentDictionaryYcsb)
            {
                var test = new ConcurrentDictionary_YcsbBenchmark(options.ThreadCount, options.NumaStyle, options.Distribution, options.ReadPercent);
                test.Run();
            }
        }
    }
}
