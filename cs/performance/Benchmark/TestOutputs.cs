// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using Performance.Common;

namespace FASTER.Benchmark
{
    [JsonObject(MemberSerialization.OptIn)]
    internal class TestOutputs
    {
        [JsonProperty]
        public OperationResults InitialInserts { get; set; } = new OperationResults();

        [JsonProperty]
        public OperationResults Transactions { get; set; } = new OperationResults();

        // Unlike PerfTest, Benchmark runs for a specific amount of time rather than a specific number
        // of transactions. In order to have a true mean (rather than average of averages) on Merge(),
        // we track the actual execution time (which will not be exactly the specified time) and the
        // actual number of transactions. These are indexed in parallel with InitialInserts and Transactions,
        // all on a per-iteration basis.

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double[] TransactionSecondsFull { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double[] TransactionSecondsTrimmed { get; set; }

        [JsonProperty]
        public long[] TransactionCountsFull { get; set; }

        [JsonProperty]
        public long[] TransactionCountsTrimmed { get; set; }
    }
}
