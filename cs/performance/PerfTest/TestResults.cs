// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using Performance.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FASTER.PerfTest
{
    [JsonObject(MemberSerialization.OptIn)]
    internal partial class TestResult
    {
        [JsonProperty]
        internal TestInputs Inputs { get; set; } = new TestInputs();

        [JsonProperty]
        internal TestOutputs Outputs { get; set; } = new TestOutputs();

        // Override equivalence testing
        public override int GetHashCode() => this.Inputs.MemberTuple.GetHashCode();

        public override bool Equals(object obj) => this.Equals(obj as TestResult);

        public bool Equals(TestResult other) => other is null ? false : this.Inputs.MemberTuple == other.Inputs.MemberTuple;
    }

    [JsonObject(MemberSerialization.OptIn)]
    class TestResults
    {
        [JsonProperty]
        public TestResult[] Results { get; set; }

        internal void Add(TestResult result)
            => this.Results = this.Results is null ? new[] { result } : this.Results.Concat(new[] { result }).ToArray();

        internal void Write(string filename) 
            => JsonUtils.WriteAllText(filename, JsonConvert.SerializeObject(this, JsonUtils.OutputSerializerSettings));

        internal static TestResults Read(string filename) 
            => JsonConvert.DeserializeObject<TestResults>(File.ReadAllText(filename), JsonUtils.InputSerializerSettings);

        internal (IEnumerable<(TestResult, TestResult)>, IEnumerable<TestResult>, IEnumerable<TestResult>) Match(TestResults other)
        {
            var thisResults = new HashSet<TestResult>(this.Results);
            var otherResults = new HashSet<TestResult>(other.Results);

            IEnumerable<(TestResult, TestResult)> zipMatchingResults()
            {
                foreach (var otherResult in other.Results)
                {
                    if (thisResults.TryGetValue(otherResult, out TestResult thisResult))
                        yield return (thisResult, otherResult);
                }
            }
            return (zipMatchingResults(), thisResults.Except(otherResults), otherResults.Except(this.Results));
        }

        internal TestResultComparisons CompareExact(TestResults other)
            => CompareSequence(Match(other).Item1);        // Compare only TestResults with matching parameters.

        internal TestResultComparisons CompareSequence(TestResults other) 
            => CompareSequence(Enumerable.Range(0, Math.Min(this.Results.Length, other.Results.Length)).Select(ii => (this.Results[ii], other.Results[ii])));

        internal static TestResultComparisons CompareSequence(IEnumerable<(TestResult, TestResult)> results) 
            => new TestResultComparisons { ResultComparisons = results.Select(result => new TestResultComparison(result.Item1, result.Item2)).ToArray() };

        internal static void Merge(string[] fileSpecs, bool intersect, string resultsFilename)
        {
            static IEnumerable<string> enumFiles(string fileSpec) 
                => Directory.EnumerateFiles(Path.GetDirectoryName(fileSpec), Path.GetFileName(fileSpec));

            var filenames = fileSpecs.SelectMany(spec => enumFiles(spec)).ToArray();
            if (filenames.Length < 2)
            {
                Console.WriteLine($"{PerfTest.MergeResultsArg} file specification did not evaluate to multiple files");
                return;
            }

            Console.WriteLine($"Merging results files");
            Console.WriteLine($"Initial file: {filenames[0]}");
            var mergedResults = Read(filenames[0]);
            for (var ii = 1; ii < filenames.Length; ++ii)
            {
                Console.WriteLine($"Merging file: {filenames[ii]}");
                mergedResults = mergedResults.Merge(Read(filenames[ii]), intersect);
            }
            mergedResults.Write(resultsFilename);
        }

        internal TestResults Merge(TestResults other, bool intersect)
        {
            var (matchPairs, thisOnly, otherOnly) = Match(other);

            var results = new List<TestResult>();

            static ResultStats mergeStats(int opsPerIter, ResultStats first, ResultStats second) 
                => ResultStats.Create(opsPerIter, 
                                      (first.OperationsPerSecond.Length, second.OperationsPerSecond.Length) switch
                                       {
                                           (0, _) => second.OperationsPerSecond,
                                           (_, 0) => first.OperationsPerSecond,
                                           _      => first.OperationsPerSecond.Concat(second.OperationsPerSecond).ToArray()
                                       });

            foreach (var (first, second) in matchPairs)
            {
                var merged = new TestResult() { Inputs = first.Inputs };

                void merge(Func<TestOutputs, OperationResults> opResultsSelector, Func<int> opsPerIterSelector)
                {
                    var opsPerIter = opsPerIterSelector();
                    var firstOpResults = opResultsSelector(first.Outputs);
                    var secondOpResults = opResultsSelector(second.Outputs);
                    var mergedOpResults = opResultsSelector(merged.Outputs);
                    mergedOpResults.AllThreadsFull = mergeStats(opsPerIter, firstOpResults.AllThreadsFull, secondOpResults.AllThreadsFull);
                    mergedOpResults.AllThreadsTrimmed = mergeStats(opsPerIter, firstOpResults.AllThreadsTrimmed, secondOpResults.AllThreadsTrimmed);
                    mergedOpResults.PerThreadFull = mergeStats(opsPerIter, firstOpResults.PerThreadFull, secondOpResults.PerThreadFull);
                    mergedOpResults.PerThreadTrimmed = mergeStats(opsPerIter, firstOpResults.PerThreadTrimmed, secondOpResults.PerThreadTrimmed);
                }

                merge(outputs => outputs.InitialInserts, () => first.Inputs.InitKeyCount);
                merge(outputs => outputs.TotalOperations, () => first.Inputs.TotalOperationCount);
                merge(outputs => outputs.Upserts, () => first.Inputs.UpsertCount);
                merge(outputs => outputs.Reads, () => first.Inputs.ReadCount);
                merge(outputs => outputs.RMWs, () => first.Inputs.RMWCount);
                results.Add(merged);
            }

            var matchCount = results.Count;
            if (!intersect)
                results.AddRange(thisOnly);
            var thisOnlyCount = results.Count - matchCount;
            if (!intersect)
                results.AddRange(otherOnly);
            var otherOnlyCount = results.Count - thisOnlyCount - matchCount;

            // Report how many on either side did not match.
            Console.WriteLine($"Merge complete: matched {matchCount}, first file unmatched {thisOnlyCount}," +
                              $" second file unmatched {otherOnlyCount}");

            return new TestResults { Results = results.ToArray() };
        }
    }
}
