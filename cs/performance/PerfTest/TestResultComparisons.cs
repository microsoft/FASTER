// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using System.IO;
using System.Linq;

namespace FASTER.PerfTest
{
    [JsonObject(MemberSerialization.OptIn)]
    class TestResultComparison
    {
        [JsonProperty]
        public TestResult First { get; set; }

        [JsonProperty]
        public TestResult Second { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double InitialInsertsPerSecondDiff { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double InitialInsertsPerSecondDiffPercent { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double InitialInsertsPerSecondPerThreadDiff { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double InitialInsertsPerSecondPerThreadDiffPercent { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double TotalOperationsPerSecondDiff { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double TotalOperationsPerSecondDiffPercent { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double TotalOperationsPerSecondPerThreadDiff { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double TotalOperationsPerSecondPerThreadDiffPercent { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double UpsertsPerSecondDiff { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double UpsertsPerSecondDiffPercent { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double UpsertsPerSecondPerThreadDiff { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double UpsertsPerSecondPerThreadDiffPercent { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double ReadsPerSecondDiff { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double ReadsPerSecondDiffPercent { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double ReadsPerSecondPerThreadDiff { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double ReadsPerSecondPerThreadDiffPercent { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double RMWsPerSecondDiff { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double RMWsPerSecondDiffPercent { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double RMWsPerSecondPerThreadDiff { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double RMWsPerSecondPerThreadDiffPercent { get; set; }

        public TestResultComparison() { } // For JSON

        public TestResultComparison(TestResult first, TestResult second)
        {
            // Speed is calculated as "Second minus First", so if Second is faster, the results show as positive (more operations per second).
            this.First = first;
            this.Second = second;

            this.InitialInsertsPerSecondDiff = second.InitialInsertsPerSecond - first.InitialInsertsPerSecond;
            this.InitialInsertsPerSecondDiffPercent = (this.InitialInsertsPerSecondDiff / first.InitialInsertsPerSecond) * 100;
            this.InitialInsertsPerSecondPerThreadDiff = second.InitialInsertsPerSecondPerThread - first.InitialInsertsPerSecondPerThread;
            this.InitialInsertsPerSecondPerThreadDiffPercent = (this.InitialInsertsPerSecondPerThreadDiff / first.InitialInsertsPerSecondPerThread) * 100;

            this.TotalOperationsPerSecondDiff = second.TotalOperationsPerSecond - first.TotalOperationsPerSecond;
            this.TotalOperationsPerSecondDiffPercent = (this.TotalOperationsPerSecondDiff / first.TotalOperationsPerSecond) * 100;
            this.TotalOperationsPerSecondPerThreadDiff = second.TotalOperationsPerSecondPerThread - first.TotalOperationsPerSecondPerThread;
            this.TotalOperationsPerSecondPerThreadDiffPercent = (this.TotalOperationsPerSecondPerThreadDiff / first.TotalOperationsPerSecondPerThread) * 100;

            this.UpsertsPerSecondDiff = second.UpsertsPerSecond - first.UpsertsPerSecond;
            this.UpsertsPerSecondDiffPercent = (this.UpsertsPerSecondDiff / first.UpsertsPerSecond) * 100;
            this.UpsertsPerSecondPerThreadDiff = second.UpsertsPerSecondPerThread - first.UpsertsPerSecondPerThread;
            this.UpsertsPerSecondPerThreadDiffPercent = (this.UpsertsPerSecondPerThreadDiff / first.UpsertsPerSecondPerThread) * 100;

            this.ReadsPerSecondDiff = second.ReadsPerSecond - first.ReadsPerSecond;
            this.ReadsPerSecondDiffPercent = (this.ReadsPerSecondDiff / first.ReadsPerSecond) * 100;
            this.ReadsPerSecondPerThreadDiff = second.ReadsPerSecondPerThread - first.ReadsPerSecondPerThread;
            this.ReadsPerSecondPerThreadDiffPercent = (this.ReadsPerSecondPerThreadDiff / first.ReadsPerSecondPerThread) * 100;

            this.RMWsPerSecondDiff = second.RMWsPerSecond - first.RMWsPerSecond;
            this.RMWsPerSecondDiffPercent = (this.RMWsPerSecondDiff / first.RMWsPerSecond) * 100;
            this.RMWsPerSecondPerThreadDiff = second.RMWsPerSecondPerThread - first.RMWsPerSecondPerThread;
            this.RMWsPerSecondPerThreadDiffPercent = (this.RMWsPerSecondPerThreadDiff / first.RMWsPerSecondPerThread) * 100;
        }

        internal void Write(string filename) 
            => File.WriteAllText(filename, JsonConvert.SerializeObject(this, Globals.jsonSerializerSettings));

        internal static TestResultComparison Read(string filename) 
            => JsonConvert.DeserializeObject<TestResultComparison>(File.ReadAllText(filename));
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

        internal void Write(string filename) 
            => File.WriteAllText(filename, JsonConvert.SerializeObject(this, Globals.jsonSerializerSettings));

        internal static TestResultComparisons Read(string filename) 
            => JsonConvert.DeserializeObject<TestResultComparisons>(File.ReadAllText(filename));
    }
}
