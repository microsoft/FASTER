// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using CommandLine;

namespace FASTER.benchmark
{
    class Options
    {
        [Option('b', "benchmark", Required = false, Default = 0,
        HelpText = "Benchmark to run (0 - YCSB)")]
        public int Benchmark { get; set; }

        [Option('t', "threads", Required = false, Default = 1,
         HelpText = "Number of threads to run the workload.")]
        public int ThreadCount { get; set; }

        [Option('n', "numa", Required = false, Default = 0,
             HelpText = "0 = no numa, 1 = sharded numa")]
        public int NumaStyle { get; set; }

        [Option('r', "read_percent", Required = false, Default = 50,
         HelpText = "Percentage of reads (-1 for 100% RMW")]
        public int ReadPercent { get; set; }

        [Option('d', "distribution", Required = false, Default = "uniform",
            HelpText = "Distribution")]
        public string Distribution { get; set; }
    }

    enum BenchmarkType : int
    {
        Ycsb
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

            Console.WriteLine("#threads = {0}", options.ThreadCount);


            if (b == BenchmarkType.Ycsb)
            {
                var test = new FASTER_YcsbBenchmark(options.ThreadCount, options.NumaStyle, options.Distribution, options.ReadPercent);
                test.Run();
            }
        }
    }
}
