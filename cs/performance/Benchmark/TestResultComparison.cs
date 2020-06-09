// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using Performance.Common;
using System;
using System.IO;

namespace FASTER.Benchmark
{
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

        // Unlike PerfTest, Benchmark has only a single TestResultComparison, not a collection of TestResultComparisons,
        // so we compare directly into the TestResultComparison object.

        private TestResultComparison(TestResult first, TestResult second)
        {
            // Speed is calculated as "Second minus First", so if Second is faster, the results show as positive (more operations per second).
            this.First = first;
            this.Second = second;

            void compare(Func<TestOutputs, OperationResults> opResultsSelector,
                         Func<OperationComparisons, OperationComparison> opComparisonSelector)
            {
                OperationResults leftResults = opResultsSelector(first.Outputs);
                OperationResults rightResults = opResultsSelector(second.Outputs);

                OperationComparison opComp = opComparisonSelector(this.OperationComparisons);
                opComp.AllThreadsFull = OperationComparisonStats.Create(leftResults.AllThreadsFull, rightResults.AllThreadsFull);
                opComp.AllThreadsTrimmed = OperationComparisonStats.Create(leftResults.AllThreadsTrimmed, rightResults.AllThreadsTrimmed);
                opComp.PerThreadFull = OperationComparisonStats.Create(leftResults.PerThreadFull, rightResults.PerThreadFull);
                opComp.PerThreadTrimmed = OperationComparisonStats.Create(leftResults.PerThreadTrimmed, rightResults.PerThreadTrimmed);
            }

            compare(outputs => outputs.InitialInserts, opc => opc.InitialInserts);
            compare(outputs => outputs.Transactions, opc => opc.Transactions);
        }

        internal static void Compare(string firstFile, string secondFile, string resultsFile)
        {
            var firstResult = TestResult.Read(firstFile);
            var secondResult = TestResult.Read(secondFile);
            if (!firstResult.Equals(secondResult))  // Compares Inputs only
                Console.WriteLine("WARNING: TestResult Inputs do not match");
            var comp = new TestResultComparison(firstResult, secondResult);
            comp.Write(resultsFile);
        }

        internal void Write(string filename)
            => JsonUtils.WriteAllText(filename, JsonConvert.SerializeObject(this, JsonUtils.OutputSerializerSettings));

        internal static TestResultComparison Read(string filename)
            => JsonConvert.DeserializeObject<TestResultComparison>(File.ReadAllText(filename), JsonUtils.InputSerializerSettings);
    }

    [JsonObject(MemberSerialization.OptIn)]
    class OperationComparisons
    {
        [JsonProperty]
        public OperationComparison InitialInserts { get; set; } = new OperationComparison();

        [JsonProperty]
        public OperationComparison Transactions { get; set; } = new OperationComparison();
    }
}
