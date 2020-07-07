// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;
using Performance.Common;
using System;

namespace FASTER.Benchmark
{
    public class Options
    {
        public const string UniformDistributionString = "uniform";
        public const string ZipfDistributionString = "zipf";

        public const int DefaultBenchmarkType = 0;
        public const int DefaultThreadCount = 8;
        public const int DefaultNumaStyle = 0;
        public const NumaMode DefaultNumaMode = NumaMode.RoundRobin;
        public const int DefaultReadPercent = 50;
        public const string DefaultDistributionString = UniformDistributionString;
        public const Distribution DefaultDistribution = Performance.Common.Distribution.Uniform;
        public const int DefaultIterationCount = 1;
        public const int DefaultRunSeconds = 30;

        public const char BenchmarkTypeArg = 'b';
        public const char ThreadCountArg = 't';
        public const char NumaStyleArg = 'n';
        public const char ReadPercentArg = 'r';
        public const char DistributionArg = 'd';
        public const char DistributionSeedArg = 's';
        public const char IterationsArg = 'i';
        public const char RunSecondsArg = 'e';

        public const char MergeArg = 'm';
        public const char CompareArg = 'c';
        public const char ResultsArg = 'o';

        [Option(BenchmarkTypeArg, "benchmark", Required = false, Default = DefaultBenchmarkType,
        HelpText = "Benchmark to run (0 = YCSB, 1 = ConcurrentDictionaryYCSB)")]
        public int Benchmark { get; set; }

        [Option(ThreadCountArg, "threads", Required = false, Default = DefaultThreadCount,
         HelpText = "Number of threads to run the workload on")]
        public int ThreadCount { get; set; }

        [Option(NumaStyleArg, "numa", Required = false, Default = DefaultNumaStyle,
             HelpText = "0 = No sharding across NUMA sockets, 1 = Sharding across NUMA sockets")]
        public int NumaStyle { get; set; }

        [Option(ReadPercentArg, "read_percent", Required = false, Default = DefaultReadPercent,
         HelpText = "Percentage of reads (-1 for 100% read-modify-write)")]
        public int ReadPercent { get; set; }

        [Option(DistributionArg, "distribution", Required = false, Default = DefaultDistributionString,
            HelpText = "Distribution of keys in workload")]
        public string Distribution { get; set; }

        [Option(DistributionSeedArg, "distributionseed", Required = false, Default = RandomGenerator.DefaultDistributionSeed,
            HelpText = "Seed for synthetic Distribution; if <= 0, the current timestamp is used")]
        public uint DistributionSeed { get; set; }

        [Option(IterationsArg, "iterations", Required = false, Default = DefaultIterationCount,
         HelpText = "Number of iterations of the test to run")]
        public int IterationCount { get; set; }

        [Option(RunSecondsArg, "seconds", Required = false, Default = DefaultRunSeconds,
         HelpText = "Number of seconds for the test to run transactions")]
        public int RunSeconds { get; set; }

        [Option(MergeArg, "merge", Required = false, Default = "",
         HelpText = "Result files to merge (must match on Inputs); use ';' to delimit filenames; must be at least two files")]
        public string MergeFiles { get; set; }

        [Option(CompareArg, "compare", Required = false, Default = "",
         HelpText = "Result files to compare (need not match on Inputs); use ';' to delimit filenames; must be two files")]
        public string CompareFiles { get; set; }

        [Option(ResultsArg, "output", Required = false, Default = "",
         HelpText = "Output result file name; required for merge or compare")]
        public string ResultsFile { get; set; }

        internal NumaMode GetNumaMode() => this.NumaStyle == 0 ? NumaMode.RoundRobin : NumaMode.Sharded2;

        internal Distribution GetDistribution() 
            => this.Distribution.ToLower() == UniformDistributionString
                ? Performance.Common.Distribution.Uniform
                : Performance.Common.Distribution.ZipfSmooth;

        internal TestInputs GetTestInputs()
            => new TestInputs
            {
                InitKeyCount = FASTER_YcsbBenchmark.kInitCount,
                TransactionKeyCount = FASTER_YcsbBenchmark.kTxnCount,
                ThreadCount = this.ThreadCount,
                NumaMode = this.GetNumaMode(),
                ReadPercent = this.ReadPercent,
                Distribution = this.GetDistribution(),
                DistributionSeed = this.DistributionSeed,
                IterationCount = this.IterationCount,
                RunSeconds = this.RunSeconds
            };

        private static bool GetFileSpecs(string property, out string[] fileSpecs)
        {
            fileSpecs = string.IsNullOrEmpty(property) ? null : property.Split(';');
            return !string.IsNullOrEmpty(property);
        }

        internal bool GetMergeFileSpecs(out string[] fileSpecs) => GetFileSpecs(this.MergeFiles, out fileSpecs);

        internal bool GetCompareFileSpecs(out string[] fileSpecs) => GetFileSpecs(this.CompareFiles, out fileSpecs);

        public bool Verify()
        {
            static bool fail(string message)
            {
                Console.WriteLine(message);
                return false;
            }

            if (!Enum.IsDefined(typeof(BenchmarkType), this.Benchmark))
                return fail($"Invalid {nameof(this.Benchmark)}: {nameof(this.Benchmark)}");

            if (this.NumaStyle < 0 || this.NumaStyle > 1)
                return fail($"Invalid {nameof(this.NumaStyle)}: Must be 0 (non-sharded) or 1 (sharded)");

            if (this.ReadPercent < -1 || this.ReadPercent > 100)
                return fail($"Invalid {nameof(this.ReadPercent)}: Must be between -1 (all RMW) and 0 (all reads, no upserts)");

            var distribution = this.Distribution.ToLower();
            if (distribution != Options.UniformDistributionString && distribution != Options.ZipfDistributionString)
                return fail($"Invalid {nameof(this.Distribution)}: Must be {Options.UniformDistributionString} or {Options.ZipfDistributionString}");

            if (this.IterationCount < 1)
                return fail($"Invalid {nameof(this.IterationCount)}: Must be > 0");

            if ((!string.IsNullOrEmpty(this.MergeFiles) || !string.IsNullOrEmpty(this.CompareFiles))
                && string.IsNullOrEmpty(this.ResultsFile))
                return fail($"{nameof(this.ResultsFile)} is required for {nameof(this.MergeFiles)} or {nameof(this.CompareFiles)}");

            return true;
        }
    }
}
