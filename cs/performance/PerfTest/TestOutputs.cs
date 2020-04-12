// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using System;
using System.Linq;

namespace FASTER.PerfTest
{
    [JsonObject(MemberSerialization.OptIn)]
    internal class TestOutputs
    {
        [JsonObject(MemberSerialization.OptIn)]
        internal class ResultStats
        {
            [JsonProperty]
            [JsonConverter(typeof(DoubleRoundingConverter))]
            internal double[] OperationsPerSecond { get; set; } = new double[0];

            [JsonProperty]
            [JsonConverter(typeof(DoubleRoundingConverter))]
            internal double Mean;

            [JsonProperty]
            [JsonConverter(typeof(DoubleRoundingConverter))]
            internal double StdDev;

            internal static ResultStats Create(double[] opsPerSecond)
            {
                var stats = new ResultStats { OperationsPerSecond = opsPerSecond };
                stats.Mean = stats.OperationsPerSecond.Length == 0 
                    ? 0.0 
                    : stats.OperationsPerSecond.Average();
                stats.StdDev = stats.OperationsPerSecond.Length <= 1
                    ? 0.0
                    : Math.Sqrt(stats.OperationsPerSecond.Sum(n => Math.Pow(n - stats.Mean, 2)) / stats.OperationsPerSecond.Length);
                return stats;
            }
        }

        [JsonObject(MemberSerialization.OptIn)]
        internal class OperationResults
        {
            [JsonProperty]
            internal ResultStats AllThreadsFull { get; set; } = new ResultStats();

            [JsonProperty]
            internal ResultStats AllThreadsTrimmed { get; set; } = new ResultStats();

            [JsonProperty]
            internal ResultStats PerThreadFull { get; set; } = new ResultStats();

            [JsonProperty]
            internal ResultStats PerThreadTrimmed { get; set; } = new ResultStats();
        }

        [JsonProperty]
        public OperationResults InitialInserts { get; set; } = new OperationResults();

        [JsonProperty]
        public OperationResults TotalOperations { get; set; } = new OperationResults();

        [JsonProperty]
        public OperationResults Upserts { get; set; } = new OperationResults();

        [JsonProperty]
        public OperationResults Reads { get; set; } = new OperationResults();

        [JsonProperty]
        public OperationResults RMWs { get; set; } = new OperationResults();
    }
}
