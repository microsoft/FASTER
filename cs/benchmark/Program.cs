// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using CommandLine;
using CommandLine.Text;

namespace FASTER.benchmark
{
    class Options
    {
        [Option('b', "benchmark", Required = false, Default = 0,
        HelpText = "Benchmark to run (0 = YCSB)")]
        public int Benchmark { get; set; }

        [Option('t', "threads", Required = false, Default = 1,
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

            result.WithNotParsed(errs =>
            {
                var helpText = HelpText.AutoBuild(result, h =>
                {
                    return HelpText.DefaultParsingErrorsHandler(result, h);
                }, e =>
                {
                    return e;
                });
                Console.WriteLine(helpText);
            });

            var options = result.MapResult(o => o, xs => new Options());
            BenchmarkType b = (BenchmarkType)options.Benchmark;

            Console.WriteLine("Benchmark Arguments:");
            Console.WriteLine("  Benchmark = {0}", options.Benchmark);
            Console.WriteLine("  Number of threads = {0}", options.ThreadCount);
            Console.WriteLine("  Thread NUMA mapping = {0}", options.NumaStyle);
            Console.WriteLine("  Read percent = {0}", options.ReadPercent);
            Console.WriteLine("  Distribution = {0}", options.Distribution);


            if (b == BenchmarkType.Ycsb)
            {
                var test = new FASTER_YcsbBenchmark(options.ThreadCount, options.NumaStyle, options.Distribution, options.ReadPercent);
                test.Run();
            }
        }
    }
}
