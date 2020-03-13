// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using System.IO;
using System.Linq;

namespace PerfTest
{
    [JsonObject(MemberSerialization.OptIn)]
    class TestResultComparison
    {
        [JsonProperty]
        public TestResult First { get; set; }

        [JsonProperty]
        public TestResult Second { get; set; }

        [JsonProperty]
        public double UpsertsPerSecondDiff { get; set; }

        [JsonProperty]
        public double UpsertsPerSecondDiffPercent { get; set; }

        [JsonProperty]
        public double ReadsPerSecondDiff { get; set; }

        [JsonProperty]
        public double ReadsPerSecondDiffPercent { get; set; }

        [JsonProperty]
        public double PendingReadsPerIterationDiff { get; set; }

        [JsonProperty]
        public double PendingReadsPerIterationDiffPercent { get; set; }

        public TestResultComparison() { } // For JSON

        public TestResultComparison(TestResult first, TestResult second)
        {
            // Speed is calculated as "Second minus First", so if Second is faster, the results show as positive (more operations per second).
            this.First = first;
            this.Second = second;
            this.UpsertsPerSecondDiff = second.UpsertsPerSecond - first.UpsertsPerSecond;
            this.UpsertsPerSecondDiffPercent = (this.UpsertsPerSecondDiff / first.UpsertsPerSecond) * 100;
            this.ReadsPerSecondDiff = second.ReadsPerSecond - first.ReadsPerSecond;
            this.ReadsPerSecondDiffPercent = (this.ReadsPerSecondDiff / first.ReadsPerSecond) * 100;
            this.PendingReadsPerIterationDiff = second.PendingReadsPerIteration - first.PendingReadsPerIteration;
            this.PendingReadsPerIterationDiffPercent = first.PendingReadsPerIteration == 0 ? 0 : (this.PendingReadsPerIterationDiff / first.PendingReadsPerIteration) * 100;
        }

        internal void Write(string filename) => File.WriteAllText(filename, JsonConvert.SerializeObject(this, PerfTest.jsonSerializerSettings));

        internal static TestResultComparison Read(string filename) => JsonConvert.DeserializeObject<TestResultComparison>(File.ReadAllText(filename));
    }

    [JsonObject(MemberSerialization.OptIn)]
    class TestResultComparisons
    {
        [JsonProperty]
        public TestResultComparison[] Comparisons { get; set; }

        internal void Add(TestResultComparison comparison) 
            => this.Comparisons = this.Comparisons is null ? new[] { comparison } : this.Comparisons.Concat(new[] { comparison }).ToArray();

        internal static void Compare(string firstFile, string secondFile, ResultComparisonMode comparisonMode, string resultsFile)
        {
            var firstResults = TestResults.Read(firstFile);
            var secondResults = TestResults.Read(secondFile);
            var comparisons = comparisonMode == ResultComparisonMode.Exact
                ? firstResults.CompareExact(secondResults)
                : firstResults.CompareSequence(secondResults);
            comparisons.Write(resultsFile);
        }

        internal void Write(string filename) => File.WriteAllText(filename, JsonConvert.SerializeObject(this, PerfTest.jsonSerializerSettings));

        internal static TestResultComparisons Read(string filename) => JsonConvert.DeserializeObject<TestResultComparisons>(File.ReadAllText(filename));
    }
}
