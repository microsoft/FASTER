// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;

namespace Performance.Common
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

        static internal OperationComparisonStats Create(ResultStats leftResultStats, ResultStats rightResultStats)
        {
            var stats = new OperationComparisonStats
            {
                MeanDiff = rightResultStats.Mean - leftResultStats.Mean,
                StdDevDiff = rightResultStats.StdDev - leftResultStats.StdDev
            };
            stats.MeanDiffPercent = leftResultStats.Mean == 0.0 ? 0.0 : (stats.MeanDiff / leftResultStats.Mean) * 100;
            stats.StdDevDiffPercent = leftResultStats.StdDev == 0.0 ? 0.0 : (stats.StdDevDiff / leftResultStats.StdDev) * 100;
            return stats;
        }
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
        internal int Rank { get; set; } = -1;
    }
}
