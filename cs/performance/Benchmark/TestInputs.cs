// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using Performance.Common;
using System;

namespace FASTER.Benchmark
{
    [JsonObject(MemberSerialization.OptIn)]
    internal class TestInputs
    {
        [JsonProperty]
        public long InitKeyCount { get; set; } = FASTER_YcsbBenchmark.kInitCount;

        [JsonProperty]
        public long TransactionKeyCount { get; set; } = FASTER_YcsbBenchmark.kTxnCount;

        [JsonProperty]
        public int ThreadCount { get; set; } = Options.DefaultThreadCount;

        [JsonProperty]
        public NumaMode NumaMode { get; set; } = Options.DefaultNumaMode;

        [JsonProperty]
        public int ReadPercent { get; set; } = Options.DefaultReadPercent;

        [JsonProperty]
        public Distribution Distribution { get; set; } = Options.DefaultDistribution;

        [JsonProperty]
        public int IterationCount { get; set; } = Options.DefaultIterationCount;

        [JsonProperty]
        public int RunSeconds { get; set; } = Options.DefaultRunSeconds;

        public bool Verify()
        {
            static bool fail(string message)
            {
                Console.WriteLine(message);
                return false;
            }

            if (this.ReadPercent < -1 || this.ReadPercent > 100)
                return fail($"Invalid {nameof(this.ReadPercent)}: Must be between -1 (all RMW) and 0 (all reads, no upserts)");

            if (this.Distribution != Distribution.Uniform && this.Distribution != Distribution.ZipfSmooth)
                return fail($"Invalid {nameof(this.Distribution)}: Must be {Distribution.Uniform} or {Distribution.ZipfSmooth}");

            if (this.IterationCount < 1)
                return fail($"Invalid {nameof(this.IterationCount)}: Must be > 0");

            return true;
        }

        public override string ToString()
            => $" {Options.ThreadCountArg} {this.ThreadCount}" +
                $" {Options.NumaStyleArg} {this.NumaMode}" +
                $" {Options.ReadPercentArg} {this.ReadPercent}" +
                $" {Options.DistributionArg} {this.Distribution}" +
                $" {Options.IterationsArg} {this.IterationCount}"
            ;

        internal (int, NumaMode, int, Distribution, int) MemberTuple
            => (this.ThreadCount, this.NumaMode, this.ReadPercent, this.Distribution, this.IterationCount);
    }
}
