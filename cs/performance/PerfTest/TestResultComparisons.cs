// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using System;
using System.IO;
using System.Linq;

namespace FASTER.PerfTest
{
    using OperationResults = TestOutputs.OperationResults;
    using ResultStats = TestOutputs.ResultStats;

    [JsonObject(MemberSerialization.OptIn)]
    internal partial class TestResultComparison
    {
        [JsonProperty]
        public TestResult First { get; set; }

        [JsonProperty]
        public TestResult Second { get; set; }

        [JsonProperty]
        public OperationComparisons OperationComparisons { get; set; } = new OperationComparisons();

        public TestResultComparison() { } // Needed for JSON because we define one taking params

        public TestResultComparison(TestResult first, TestResult second)
        {
            // Speed is calculated as "Second minus First", so if Second is faster, the results show as positive (more operations per second).
            this.First = first;
            this.Second = second;

            void compare(Func<TestOutputs, OperationResults> opResultsSelector,
                         Func<OperationComparisons, OperationComparison> opComparisonSelector)
            {
                OperationResults leftResults = opResultsSelector(first.Outputs);
                OperationResults rightResults = opResultsSelector(second.Outputs);

                static OperationComparisonStats createComparisonStats(ResultStats leftResultStats, ResultStats rightResultStats)
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

                OperationComparison opComp = opComparisonSelector(this.OperationComparisons);
                opComp.AllThreadsFull = createComparisonStats(leftResults.AllThreadsFull, rightResults.AllThreadsFull);
                opComp.AllThreadsTrimmed = createComparisonStats(leftResults.AllThreadsTrimmed, rightResults.AllThreadsTrimmed);
                opComp.PerThreadFull = createComparisonStats(leftResults.PerThreadFull, rightResults.PerThreadFull);
                opComp.PerThreadTrimmed = createComparisonStats(leftResults.PerThreadTrimmed, rightResults.PerThreadTrimmed);
            }

            compare(outputs => outputs.InitialInserts, opc => opc.InitialInserts);
            compare(outputs => outputs.TotalOperations, opc => opc.TotalOperations);
            compare(outputs => outputs.Upserts, opc => opc.Upserts);
            compare(outputs => outputs.Reads, opc => opc.Reads);
            compare(outputs => outputs.RMWs, opc => opc.RMWs);
        }
    }

    [JsonObject(MemberSerialization.OptIn)]
    class OperationComparisons
    {
        [JsonProperty]
        public OperationComparison InitialInserts { get; set; } = new OperationComparison();

        [JsonProperty]
        public OperationComparison TotalOperations { get; set; } = new OperationComparison();

        [JsonProperty]
        public OperationComparison Upserts { get; set; } = new OperationComparison();

        [JsonProperty]
        public OperationComparison Reads { get; set; } = new OperationComparison();

        [JsonProperty]
        public OperationComparison RMWs { get; set; } = new OperationComparison();
    }

    [JsonObject(MemberSerialization.OptIn)]
    class TestResultComparisons
    {
        [JsonProperty]
        public TestResultComparison[] ResultComparisons { get; set; }

        internal static void Compare(string firstFile, string secondFile, ResultComparisonMode comparisonMode, string resultsFile)
        {
            var firstResults = TestResults.Read(firstFile);
            var secondResults = TestResults.Read(secondFile);
            var comparisons = comparisonMode == ResultComparisonMode.Exact
                ? firstResults.CompareExact(secondResults)
                : firstResults.CompareSequence(secondResults);

            // Report how many on either side did not match.
            if (comparisonMode == ResultComparisonMode.Exact)
            {
                Console.Write($"{comparisons.ResultComparisons.Length} exact result comparisons matched. Mismatches: ");
                var firstDiff = firstResults.Results.Length - comparisons.ResultComparisons.Length;
                var secondDiff = secondResults.Results.Length - comparisons.ResultComparisons.Length;
                Console.WriteLine(firstDiff == 0 && secondDiff == 0
                                  ? "None"
                                  : $" first file {firstDiff}, second file {secondDiff}");
            }
            else
            {
                var diff = firstResults.Results.Length - secondResults.Results.Length;
                var longFile = diff > 0 ? "first" : "second";
                Console.Write($"{comparisons.ResultComparisons.Length} sequential result comparisons matched. Mismatches: ");
                Console.WriteLine(diff == 0
                                  ? "None"
                                  : $"{longFile} {Math.Abs(diff)}");
            }

            if (comparisons.ResultComparisons.Length > 0)
            {
                // Rank within each operation, across all parameter permutations.
                void rank(Func<OperationComparisons, OperationComparison> opComparisonSelector)
                {
                    foreach (var pair in comparisons.ResultComparisons.Select(opc => opComparisonSelector(opc.OperationComparisons))
                                                                .OrderByDescending(op => op.AllThreadsTrimmed.MeanDiffPercent)
                                                                .Select((op, rank) => new { op, rank }))
                    {
                        // We base the rank on the trimmed (outliers excluded, if 3 or more) results.
                        pair.op.Rank = pair.rank;
                    }
                }

                rank(opc => opc.InitialInserts);
                rank(opc => opc.TotalOperations);
                rank(opc => opc.Upserts);
                rank(opc => opc.Reads);
                rank(opc => opc.RMWs);
            }

            comparisons.Write(resultsFile);
        }

        internal void Write(string filename) 
            => File.WriteAllText(filename, JsonConvert.SerializeObject(this, Globals.outputJsonSerializerSettings));

        internal static TestResultComparisons Read(string filename) 
            => JsonConvert.DeserializeObject<TestResultComparisons>(File.ReadAllText(filename), Globals.inputJsonSerializerSettings);
    }
}
