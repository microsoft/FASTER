// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;

namespace FASTER.PerfTest
{
    [JsonObject(MemberSerialization.OptIn)]
    internal class OperationComparisonStats
    {
        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        internal double MeanDiff { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        internal double MeanDiffPercent { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        internal double StdDevDiff { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        internal double StdDevDiffPercent { get; set; }
    }

    [JsonObject(MemberSerialization.OptIn)]
    internal class OperationComparison
    {
        [JsonProperty]
        internal OperationComparisonStats AllThreadsFull { get; set; } = new OperationComparisonStats();

        [JsonProperty]
        internal OperationComparisonStats AllThreadsTrimmed { get; set; } = new OperationComparisonStats();

        [JsonProperty]
        internal OperationComparisonStats PerThreadFull { get; set; } = new OperationComparisonStats();

        [JsonProperty]
        internal OperationComparisonStats PerThreadTrimmed { get; set; } = new OperationComparisonStats();

        [JsonProperty]
        internal int Rank { get; set; }
    }
}
